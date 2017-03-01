{-# LANGUAGE TemplateHaskell #-}
module ThequeScenario (main) where

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

testClient :: ProcessId -> Process ()
testClient m
  = do cmd  <- liftIO $ getChar
       tag  <- liftIO $ getLine
       rpc  <- case cmd of
         'a' -> do ref <- liftIO $ getLine
                   return (AddTag (TagId tag) [OtherTag (TagId ref)])
         'g' -> return (GetTag (TagId tag))
       resp <- call m rpc
       case resp of
         M.OK                    -> return ()
         M.TagRefs rs            -> return ()
         M.AddBlobServers bid ps -> return ()
remotable ['testClient]

spawnMasterClients :: ProcessId -> [NodeId] -> Process (SymSet ProcessId)
spawnMasterClients m ns
  = spawnSymmetric ns $ $(mkBriskClosure 'testClient) m

main :: Backend
     -> [NodeId]
     -> [NodeId]
     -> Process ()
main b slaveNodes clientNodes
  = do m <- getSelfPid
       spawnMasterClients m clientNodes
       runMaster b slaveNodes
