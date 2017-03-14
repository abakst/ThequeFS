{-# language TemplateHaskell #-}
{-# language DeriveGeneric #-}
{-#
  options_ghc -fplugin      Brisk.Plugin
              -fplugin-opt  Brisk.Plugin:runMaster
#-}
module Theque.Thequefs.Master where

import Control.Monad (forM, foldM)
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

import GHC.Base.Brisk
import Control.Distributed.BriskStatic
import Control.Distributed.Process.SymmetricProcess
import Control.Distributed.Process.Brisk hiding (call)
import Control.Distributed.Process.ManagedProcess.Brisk


import Theque.Thequefs.Types
import Theque.Thequefs.DataNode hiding (AddBlob, initState, OK)
import Theque.Thequefs.TagNode  hiding (BlobId, GetTag, initState, OK)
import qualified Theque.Thequefs.TagNode as TN

masterService :: String
masterService = "theque:master"

data MasterState = MS {
    dataServers :: SymSet ProcessId
  , tagServers  :: SymSet ProcessId
  , lastBlobNo  :: Int
  }

initState :: MasterState
initState = MS { dataServers = undefined
               , tagServers  = undefined
               , lastBlobNo  = 0
               }

data MasterAPI = AddBlob BlobId Int     -- ^ Args: Blob prefix, Replication
               | GetTag  TagId          -- ^ Args: Tag Id to fetch
               | AddTag  TagId [TagRef] -- ^ Args: Tag Name, List of Refs
                 deriving (Eq, Ord, Show, Generic)

data MasterResponse = OK
                    | TagRefs [TagRef]
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

addTag :: ProcessId -> TagId -> [TagRef] -> Process MasterResponse
addTag m tag refs
  = call m (AddTag tag refs)

getTag :: ProcessId -> TagId -> Process MasterResponse
getTag m t
  = call m (GetTag t)

tagServer :: () -> Process ()
tagServer () = runTagNode

dataServer :: () -> Process ()
dataServer () = runDataNode

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
  ts <- spawnSymmetric ns $ $(mkBriskClosure 'tagServer) ()
  serve initState (initializeMaster ts) (masterProcess ts)

initializeMaster :: SymSet ProcessId -> MasterState -> Process (InitResult MasterState)
initializeMaster ts s
  = do us <- getSelfPid
       -- ts <- spawnSymmetric ns $ $(mkBriskClosure 'tagServer) ()
       -- bs <- spawnSymmetric ns $ $(mkBriskClosure 'dataServer) ()
       let s' = s { tagServers  = ts
                  , dataServers = empty -- bs
                  }
       return $ InitOk s' NoDelay

masterProcess :: SymSet ProcessId -> ProcessDefinition MasterState
masterProcess ts = defaultProcess {
  apiHandlers = [masterAPIHandler ts]
  }

masterAPIHandler :: SymSet ProcessId -> Dispatcher MasterState
masterAPIHandler ts = handleCall (masterAPIHandler' ts)

type MasterReply = Process (ProcessReply MasterResponse MasterState)

masterAPIHandler' :: SymSet ProcessId -> MasterState -> MasterAPI -> MasterReply
{-
masterAPIHandler' s (AddBlob n k)
  = reply (AddBlobServers blob ts) s'
  where
    ts         = chooseDataServers k s
    (blob, s') = newBlob n s
masterAPIHandler' s (AddTag tag refs)
  -- 1. ask each tag server for current version of tag
  -- 2. choose most up-to-date??
  -- 3. modify tag store
  = do doAddTag tns tag refs
       reply OK s
  where
    go _ y = send y ()
    tns    = tagServers s
-}
masterAPIHandler' ts s (GetTag tid)
  = do res <- foldM go Nothing ts
       let refs = maybe [] tagRefs res
       reply (TagRefs refs) s
  where
    go t0 tn = do t <- TN.getTag tn tid
                  return t0
    -- go t0 tn = do t <- TN.getTag tn tid
    --               case (t0, t) of
    --                 (Just Tag{tagRev=i}, TN.TagFound t'@Tag{tagRev=j})
    --                   | j > i           -> return (Just t')
    --                 (_, TN.TagFound t') -> return (Just t')
    --                 _                   -> return t0
masterAPIHandler' _ s (AddBlob _ _)
  = reply OK s
masterAPIHandler' _ s (AddTag _ _)
  = reply OK s

doAddTag :: SymSet ProcessId -> TagId -> [TagRef] -> Process ()
doAddTag tns tag refs
  = do best <- foldM askTN Nothing tns
       foldM (setTN (tag' best)) () tns
       return ()
  where
    newtag   = Tag { tagId = tag
                   , tagRefs = refs
                   , tagRev = 0
                   }
    updtag t = t { tagRefs = tagRefs t ++ refs
                 , tagRev = tagRev t + 1
                 }
    tag' mtag
      = maybe newtag updtag $ mtag

    setTN t _ tn = TN.setTag tn tag t >> return ()

    askTN best tn
      = do tag <- TN.getTag tn tag
           case (tagRev <$> best, tag) of
             (Just i, TN.TagFound t@Tag{tagRev = i'})
               | i' > i  -> return (Just t)
             (_, TN.TagFound t@Tag{})
                         -> return (Just t)
             _           -> return best

chooseDataServers :: Int -> MasterState -> [ProcessId]
chooseDataServers k s
  = go [] k
  where
    go l 0 = l
    go l i = go (chooseSymmetric srvs i : l) (i - 1)
  -- = [ chooseSymmetric srvs i | i <- [1..k] ]
  -- where
    srvs   = dataServers s

newBlob :: String -> MasterState -> (BlobId, MasterState)
newBlob n s
  = (n', s')
  where
    n' = n ++ "__" ++ show (lastBlobNo s)
    s' = s { lastBlobNo = 1 + lastBlobNo s }
