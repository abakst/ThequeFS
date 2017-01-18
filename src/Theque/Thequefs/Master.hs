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
import Control.Distributed.Process.Extras.Time
import Control.Distributed.Process.Node
import Control.Distributed.Process.ManagedProcess
import Control.Distributed.Process.Backend.SimpleLocalnet

import Theque.Thequefs.Types
import Theque.Thequefs.DataNode (runDataNode)
import Theque.Thequefs.TagNode  (runTagNode)

masterService :: String
masterService = "theque:master"

data MasterState = MS {
    dataServers :: [ProcessId]
  , tagServers  :: [ProcessId]
  , lastBlobNo  :: Int
  }

initState :: MasterState
initState = MS { dataServers = []
               , tagServers  = []
               , lastBlobNo  = 0
               }

data MasterAPI = AddBlob BlobId Int  -- ^ Args: Blob prefix, Replication
               | TagBlobs TagId [Blob] -- ^ Args: Tag, Blobs
               | GetTag
                 deriving (Eq, Ord, Show, Generic)

data MasterResponse = OK
                    | AddBlobServers BlobId [ProcessId]
                    deriving (Eq, Ord, Show, Generic)

instance Binary MasterAPI
instance Binary MasterResponse

findMaster :: String -> Process ProcessId
findMaster srv = findService node masterService
  where -- ugh
    node = NodeId
         . EndPointAddress
         $ BS.concat [BS.pack srv, BS.pack ":1"]

addBlob :: ProcessId -> String -> Int -> Process MasterResponse
addBlob m bn k
  = call m (AddBlob bn k)

tagBlobs :: ProcessId -> TagId -> [Blob] -> Process MasterResponse
tagBlobs m tag blobs
  = call m (TagBlobs tag blobs)

tagServer :: ProcessId -> Process ()
tagServer m = say "TagServer Starting..." >> runTagNode m

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
  = reply (AddBlobServers blob ts) s''
  where
    (ts, s')    = chooseDataServers k s
    (blob, s'') = newBlob n s'
masterAPIHandler' s (TagBlobs tag blobs)
  -- 1. ask each tag server for current version of tag
  -- 2. choose most up-to-date??
  -- 3. modify tag store
  = undefined
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

newBlob :: String -> MasterState -> (BlobId, MasterState)
newBlob n s
  = (n', s')
  where
    n' = n ++ "__" ++ show (lastBlobNo s)
    s' = s { lastBlobNo = 1 + lastBlobNo s }
