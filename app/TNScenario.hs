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
import Control.Distributed.Process.ManagedProcess

import GHC.Base.Brisk
import Control.Distributed.BriskStatic
import Control.Distributed.Process.SymmetricProcess
import Control.Distributed.Process.Brisk hiding (call)
import Control.Distributed.Process.ManagedProcess.Brisk

readTagRefs :: IO [TagRef]
readTagRefs = undefined

testClient :: ProcessId -> Process ()
testClient m
  = do -- cmd  <- liftIO $ getChar
       name <- liftIO $ getLine
       -- dat  <- liftIO $ read <$> getLine
       -- refs <- liftIO $ readTagRefs
       -- rpc  <- case cmd of
         -- 'a' -> do ref <- liftIO $ getLine
         --           return (M.AddBlob name dat)
         -- 'g' -> return (GetTag (TagId name))
         -- 't' -> return (AddTag (TagId name) refs)
       let rpc = GetTag (TagId name)
       call m rpc :: Process MasterResponse
       return ()

remotable ['testClient]

main :: Backend -> NodeId -> [NodeId] -> [NodeId] -> Process ()
main be mnode slaveNodes clientNodes
  = do m  <- getSelfPid
       spawnSymmetric clientNodes $ $(mkBriskClosure 'testClient) m 
       runMaster be slaveNodes
