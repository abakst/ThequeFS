{-# LANGUAGE DeriveDataTypeable #-}
module Main where

import System.Console.CmdArgs
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

main :: IO ()
main = do
  args <- cmdArgs thequeArgs
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
    Client{host = h, port = p, masterAddr = m} -> do
      backend <- initializeBackend h p rt
      node    <- newLocalNode backend
      runClient m node
  where
    rt = E.__remoteTable
       . M.__remoteTable
       $ initRemoteTable

      
data ThequeFS = Master { host :: String, port :: String }
              | Slave  { host :: String, port :: String }
              | Client { host :: String, port :: String, masterAddr :: String }
              deriving (Show, Data, Typeable)

thequeArgs = modes [master, slave, client]
  where
    master = Master { host = "localhost" &= help "Host to run master on"
                    , port = "9001" &= help "Port to run master on"
                    }
    slave  = Slave  { host = "localhost" &= help "Host to run slave on"
                    , port = "9002" &= help "Port to run slave on"
                    }
    client = Client { host = "localhost" &= help "Host to run slave on"
                    , port = "9000" &= help "Port to run slave on"
                    , masterAddr = "localhost:9001" &= help "Address:Port of master"
                    }
