{-# OPTIONS_GHC -O0 #-}
{-# LANGUAGE TemplateHaskell #-}
module ThequeScenario (main) where

import Data.Char
import System.IO
import Control.Distributed.Process.Closure
import Control.Distributed.Process.Backend.SimpleLocalnet
import Control.Distributed.Process hiding (call)
import Theque.Thequefs.Types
import Theque.Thequefs.Client as C
import Theque.Thequefs.Master as M
import Theque.Thequefs.DataNode as DN
import Control.Distributed.Process.ManagedProcess

import GHC.Base.Brisk
import Control.Distributed.BriskStatic
import Control.Distributed.Process.SymmetricProcess
import Control.Distributed.Process.Brisk hiding (call)
import Control.Distributed.Process.ManagedProcess.Brisk

testClient :: Int -> SymSet ProcessId -> Process ()
testClient n ds
  = do cmd  <- liftIO $ getChar
       blob <- liftIO $ getLine
       dat  <- liftIO $ stringByteString <$> getLine
       c    <- liftIO $ getChar >>= return . ord
       let d   = chooseSymmetric ds (c `mod` n)
           rpc = if cmd == 'p' then 
                  DN.AddBlob blob dat
                 else
                  GetBlob blob
           -- rpc = DN.AddBlob blob dat
       call d rpc :: Process DataNodeResponse
       return ()
remotable ['testClient, 'runDataNode]

spawnDataNodes :: [NodeId] -> Process (SymSet ProcessId)
spawnDataNodes ns 
  = spawnSymmetric ns $ $(mkBriskStaticClosure 'runDataNode)

main ::[NodeId] -> Process ()
main slaveNodes 
  = do m  <- getSelfPid
       ds <- spawnDataNodes slaveNodes
       c  <- liftIO $ getChar >>= return . ord
       testClient (length slaveNodes) ds
