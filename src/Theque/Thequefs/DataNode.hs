{-#
  OPTIONS_GHC -fplugin      Brisk.Plugin
              -fplugin-opt  Brisk.Plugin:runDataNode
#-}
{-# LANGUAGE DeriveGeneric #-}
module Theque.Thequefs.DataNode where

import           Data.Binary
import           GHC.Generics (Generic)
import qualified Data.ByteString as BS
import qualified Data.HashMap.Strict as M
import           Control.Distributed.Process hiding (call)
import           Control.Distributed.Process.Extras.Time
import           Control.Distributed.Process.ManagedProcess
import           Theque.Thequefs.Types
import qualified Codec.Binary.UTF8.String as B8String

import           GHC.Base.Brisk
import           Control.Distributed.Process.Brisk hiding (call)
import           Control.Exception.Base.Brisk
import           Control.Distributed.Process.ManagedProcess.Brisk

{-
DataNode:
1. register datanode service
2. start "space reporter" ???
3. start RPC service
-}
dataNodeService :: String
dataNodeService = "theque:dataNode"

findDataNode = flip findService dataNodeService

type DataNodeMap = M.HashMap BlobId BS.ByteString

data DataNodeState = DNS {
  blobs  :: !DataNodeMap
  }

data DataNodeAPI = AddBlob String BS.ByteString
                 | GetBlob BlobId
                 deriving (Eq, Ord, Show, Generic)

stringByteString = BS.pack . B8String.encode

pushBlob :: ProcessId -> String -> BS.ByteString -> Process DataNodeResponse
pushBlob p bn bdata
  = call p (AddBlob bn bdata)

instance Binary DataNodeAPI

data DataNodeResponse = OK
                      | BlobData BS.ByteString
                      | BlobNotFound
                      | BlobExists
                      deriving (Eq, Ord, Show, Generic)
instance Binary DataNodeResponse

initState = DNS { blobs = M.empty }

{-@
  ANN runDataNode ::
        \x. while true do { msg <- recv; case msg of Foo -> ... }
@-}
runDataNode :: Process ()
runDataNode = do
  self <- getSelfPid
  register dataNodeService self
  serve initState initializeDataNode dataNodeProcess

initializeDataNode :: s -> Process (InitResult s)
initializeDataNode s = return $ InitOk s NoDelay

dataNodeProcess :: ProcessDefinition DataNodeState
dataNodeProcess = defaultProcess {
  apiHandlers = [dataNodeAPIHandler]
  }

type DataNodeReply = Process (ProcessReply DataNodeResponse DataNodeState)

dataNodeAPIHandler :: Dispatcher DataNodeState
dataNodeAPIHandler = handleCall dataNodeAPIHandler'

dataNodeAPIHandler' :: DataNodeState -> DataNodeAPI -> DataNodeReply
dataNodeAPIHandler' st (GetBlob bid)
  = reply response st
  where
     response = case M.lookup bid (blobs st) of
                  Nothing    -> BlobNotFound
                  Just bdata -> (BlobData bdata)
dataNodeAPIHandler' st (AddBlob bn blob)
  = reply response st'
  where
       (response, st') = case M.lookup bn $ blobs st of
                           Nothing ->
                             (OK, st { blobs = M.insert bn blob $ blobs st })
                           Just _ ->
                             (BlobExists, st)
