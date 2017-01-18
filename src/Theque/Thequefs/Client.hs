module Theque.Thequefs.Client where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import           Data.ByteString.Char8  (pack)
import Control.Monad (forM, forM_, when, unless)
import Control.Concurrent (threadDelay)
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node (LocalNode, runProcess)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process.ManagedProcess (call)
import System.Exit
import Data.Aeson

import           Theque.Thequefs.Types as Types
import           Theque.Thequefs.CmdLine as Cmd
import qualified Theque.Thequefs.Master as Master
import qualified Theque.Thequefs.DataNode as DataNode

kReplFactor :: Int
kReplFactor = 3

runClient :: ThequeFS -> LocalNode -> IO ()
runClient client node
  = runProcess node $ go client
  where
    go NewBlob { master = maddr, blobName = blob }
      = do s <- Master.findMaster maddr
           say (show s)
           addBlob s blob
    go PushData { node = n, Cmd.blobId = bid, Cmd.blobData = bd }
      = do pid <- DataNode.findDataNode (makeNodeId n)
           pushBlob pid bid bd

addBlob :: ProcessId -> BlobId -> Process ()
addBlob master bn
  = do r <- Master.addBlob master bn kReplFactor
       case r of
         Master.AddBlobServers bid ps ->
           let locs = [ BlobLoc { Types.blobEndPoint = (endPoint p)
                                , Types.blobId = bid }
                      | p <- ps ]
           in liftIO $ do BSL.putStr (encode locs)
                          exitSuccess

pushBlob :: ProcessId -> String -> String -> Process ()
pushBlob pid bid bd
  = do resp <- DataNode.pushBlob pid bid (pack bd)
       case resp of
         DataNode.OK         -> liftIO $ exitSuccess
         DataNode.BlobExists -> liftIO $ exitFailure
