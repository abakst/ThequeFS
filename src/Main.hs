module Main where

import System.Environment (getArgs)
import System.Console.CmdArgs

import Control.Distributed.Process
import Control.Distributed.Process.Node (initRemoteTable)
import Control.Distributed.Process.Backend.SimpleLocalnet

master :: Backend -> [NodeId] -> Process ()
master backend slaves = do
  return ()

main :: IO ()
main = do
  -- args <- getArgs

  -- case args of
  --   ["master", host, port] -> do
  --     backend <- initializeBackend host port initRemoteTable
  --     startMaster backend (master backend)
  --   ["slave", host, port] -> do
  --     backend <- initializeBackend host port initRemoteTable
  --     startSlave backend
      
data ThequeArgs = Master { host :: String, port :: String }
                | Slave  { host :: String, port :: String }
                | Client { host :: String, port :: String, masterAddr :: String }
