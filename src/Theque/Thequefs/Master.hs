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
import qualified Theque.Thequefs.DataNode as DN
import Theque.Thequefs.DataNode hiding (AddBlob, PushBlob, GetBlob, BlobExists, BlobData, initState, OK)
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
               | PushBlob String BS.ByteString
               | GetBlob BlobId
               | GetTag  TagId          -- ^ Args: Tag Id to fetch
               | AddTag  TagId [TagRef] -- ^ Args: Tag Name, List of Refs
                 deriving (Eq, Ord, Show, Generic)

data MasterResponse = OK
                    | TagRefs [TagRef]
                    | AddBlobServers BlobId [ProcessId]
                    | DNResponse DN.DataNodeResponse
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

foo :: () -> Process ()
foo () = return ()

remotable ['tagServer, 'dataServer, 'foo]
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
  bs <- spawnSymmetric ns $ $(mkBriskClosure 'dataServer) ()
  -- ts <- spawnSymmetric ns $ $(mkBriskClosure 'tagServer) ()
  ts <- spawnSymmetric ns $ $(mkBriskClosure 'foo) ()
  serve initState (initializeMaster bs ts) (masterProcess bs ts)

initializeMaster :: SymSet ProcessId -> SymSet ProcessId -> MasterState -> Process (InitResult MasterState)
initializeMaster bs ts s
  = do us <- getSelfPid
       -- ts <- spawnSymmetric ns $ $(mkBriskClosure 'tagServer) ()
       let s' = s { tagServers  = ts
                  , dataServers = bs
                  }
       return $ InitOk s' NoDelay

masterProcess :: SymSet ProcessId -> SymSet ProcessId -> ProcessDefinition MasterState
masterProcess bs ts = defaultProcess {
  apiHandlers = [masterAPIHandler bs ts]
  }

masterAPIHandler :: SymSet ProcessId -> SymSet ProcessId -> Dispatcher MasterState
masterAPIHandler bs ts = handleCall (masterAPIHandler' bs ts)

type MasterReply = Process (ProcessReply MasterResponse MasterState)

masterAPIHandler' :: SymSet ProcessId -> SymSet ProcessId -> MasterState -> MasterAPI -> MasterReply
masterAPIHandler' bns tns s (AddBlob n k)
  = reply (AddBlobServers blob ts) s'
  where
    ts         = chooseDataServers k s
    (blob, s') = newBlob n s

masterAPIHandler' bns tns s (PushBlob bid dat)
  = do foldM (\_ dn -> do pushBlob dn bid dat
                          return () ) () bns -- Replicate to EVERYONE for now
       reply OK s

masterAPIHandler' bns tns s (GetBlob bid) -- Since we're replicating to EVERYONE, need
                                          -- to query everyone
  = do b <- foldM (\r dn -> do b <- call dn (DN.GetBlob bid)
                               return (r `combine` Just b)
                  ) Nothing bns
       case b of
         Nothing -> reply (DNResponse DN.BlobNotFound) s
         Just dn -> reply (DNResponse dn) s
       reply OK s
 where
   combine Nothing x = x
   combine x y       = x

masterAPIHandler' bns tns s (AddTag addtag refs)
  -- 1. ask each tag server for current version of tag
  -- 2. choose most up-to-date??
  -- 3. modify tag store
  = do doAddTag tns addtag refs
       reply OK s

masterAPIHandler' bns ts s (GetTag tid)
  = do res <- foldM (askTN tid) Nothing ts
       let refs = maybe [] tagRefs res
       reply (TagRefs refs) s

doAddTag :: SymSet ProcessId -> TagId -> [TagRef] -> Process ()
doAddTag tns tag refs
  = do best <- foldM (askTN tag) Nothing tns
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

    setTN t _ tn = do TN.setTag tn tag t
                      return ()
askTN :: TagId -> Maybe Tag -> ProcessId -> Process (Maybe Tag)
askTN tagId best tn
  = do askedTag <- TN.getTag tn tagId
       case askedTag of
         TN.TagFound tf@Tag{tagRev = idxNew} ->
           case tagRev <$> best of
             Just idx  -> if idx < idxNew then return (Just tf) else return best
             Nothing   -> return (Just tf)
         TN.TagNotFound -> return best
         TN.OK          -> return best
         TN.TagInfo _   -> return best

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
