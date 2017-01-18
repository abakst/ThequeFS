module Main where

import System.Posix.Signals
import System.Exit
import Control.Monad.Catch as Catch (catch)
import Control.Exception (AsyncException(..))
import System.Environment (getArgs)
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Internal.Primitives
import Control.Distributed.Process.Node (initRemoteTable, runProcess)
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process.Extras as E (__remoteTable) 

import Theque.Thequefs.Master as M (runMaster, findMaster, addBlob, __remoteTable)
import Theque.Thequefs.Client as C
import Theque.Thequefs.Types
import Theque.Thequefs.CmdLine

main :: IO ()
main = do
  args <- getCmdArgs
  case args of
    Master{host = h, port = p} -> do
      backend <- initializeBackend h p rt 
      ln      <- newLocalNode backend
      Catch.catch (startMaster backend (runMaster backend))
                  (handler ln backend) 
        where
          handler ln be UserInterrupt
            = do runProcess ln (terminateAllSlaves be)
                 exitSuccess
    Slave{host = h, port = p} -> do
      backend <- initializeBackend h p rt 
      startSlave backend
    client -> do
      backend <- initializeBackend "localhost" "9000" rt
      node    <- newLocalNode backend
      runClient client node
  where
    rt = E.__remoteTable
       . M.__remoteTable
       $ initRemoteTable
