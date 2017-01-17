{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveGeneric #-}
module Theque.Thequefs.Master where

import Control.Monad (forM)
import Control.Concurrent (threadDelay)
import Data.Binary
import GHC.Generics (Generic)
import Network
import Network.Transport hiding (send)
import System.IO
import Control.Concurrent
import Control.Concurrent.STM
import qualified Data.ByteString.Char8 as BS

import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Extras (resolve)
import Control.Distributed.Process.Extras.Time
import Control.Distributed.Process.Node
import Control.Distributed.Process.ManagedProcess
import Control.Distributed.Process.Backend.SimpleLocalnet

import Theque.Thequefs.DataNode (runDataNode)

masterService :: String
masterService = "theque:master"

data MasterState = MS {
    dataServers :: [ProcessId]
  , tagServers  :: [ProcessId]
  , lastBlobNo  :: Int
  }

initState = MS { dataServers = []
               , tagServers  = []
               , lastBlobNo  = 0
               }

data MasterAPI = AddBlob String Int -- ^ Args: Blob prefix, Replication
               | TagBlobs
               | GetTag
                 deriving (Eq, Ord, Show, Generic)
instance Binary MasterAPI

findMaster :: String -> Process ProcessId
findMaster srv
  = do say "Finding Master"
       say (show n)
       Just s <- resolve (n, masterService)
       say "Found Master"
       return s
  where n = makeNodeId srv

makeNodeId :: String -> NodeId
makeNodeId addr
  = NodeId
  . EndPointAddress
  . BS.concat $ [BS.pack addr, BS.pack ":1"]

addBlob :: ProcessId -> String -> Int -> Process MasterResponse
addBlob m bn k
  = call m (AddBlob bn k)

data MasterResponse = OK
                    | AddBlobServers String [ProcessId]
                    deriving (Eq, Ord, Show, Generic)
instance Binary MasterResponse

tagServer :: () -> Process ()
tagServer _
  = do say "tag server"
       liftIO $ threadDelay 10000
       return ()

dataServer :: ProcessId -> Process ()
dataServer m = say "DataServer Starting..." >> runDataNode m
remotable ['tagServer, 'dataServer]

{-
The master must:
  0. register service
  1. start tag nodes
  2. start data nodes
  3. enter RPC loop
-}
runMaster :: Backend -> [NodeId] -> Process ()
runMaster _ ns = do
  self <- getSelfPid
  register masterService self
  say "registered"
  serve initState (initializeMaster ns) masterProcess

initializeMaster ns s
  = do us <- getSelfPid
       ts <- forM ns $ \nid -> spawn nid $ $(mkClosure 'tagServer) ()
       bs <- forM ns $ \nid -> spawn nid $ $(mkClosure 'dataServer) us
       let s' = s { tagServers  = ts
                  , dataServers = bs
                  }
       return $ InitOk s' NoDelay

masterProcess :: ProcessDefinition MasterState
masterProcess = defaultProcess {
  apiHandlers = [masterAPIHandler]
  }

masterAPIHandler :: Dispatcher MasterState
masterAPIHandler = handleCall masterAPIHandler'

type MasterReply = Process (ProcessReply MasterResponse MasterState)

masterAPIHandler' :: MasterState -> MasterAPI -> MasterReply
masterAPIHandler' s (AddBlob n k)
  = reply (AddBlobServers n' ts) s''
  where
    (ts, s') = chooseDataServers k s
    (n', s'') = newBlobName n s'
masterAPIHandler' s _
  = do say "got message"
       reply OK s

chooseDataServers k s
  | length srvs <= k
  = (srvs, s)
  | otherwise
  = (take k srvs, s { dataServers = shuffle srvs })
  where
    shuffle l = drop k l ++ take k l
    srvs      = dataServers s

newBlobName n s
  = (n', s')
  where
    n' = n ++ "__" ++ show (lastBlobNo s)
    s' = s { lastBlobNo = 1 + lastBlobNo s }
