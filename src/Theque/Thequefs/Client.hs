module Theque.Thequefs.Client where

import qualified Data.ByteString as BS
import           Data.ByteString.Char8  (pack)
import Control.Monad (forM)
import Control.Concurrent (threadDelay)
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Node (LocalNode, runProcess)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process.ManagedProcess (call)

import qualified Theque.Thequefs.Master as Master
import qualified Theque.Thequefs.DataNode as DataNode

runClient :: String -> LocalNode -> IO ()
runClient srv node
  = runProcess node $ do
      s <- Master.findMaster srv
      say (show s)
      r <- Master.addBlob s "blob" 3
      say (show r)

      case r of
        Master.AddBlobServers bn ps ->
          forM ps $ \pid -> do
            resp <- DataNode.pushBlob pid bn (pack "asdf")
            case resp of
              DataNode.OK ->
                say $ "Pushed blob to " ++ show pid
              DataNode.BlobExists ->
                say $ "Already pushed blob to " ++ show pid

      liftIO $ threadDelay 10000
      return ()
