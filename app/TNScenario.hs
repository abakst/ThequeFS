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

data Cmd = Get | Add | Alloc
  deriving (Show, Read)

testClient :: ProcessId -> Process ()
testClient masta
  = do cmd     <- liftIO $ getLine
       name    <- liftIO $ getLine
       refs    <- liftIO $ readTagRefs
       rpc     <- case read cmd of
                    Add   -> return $ AddTag (TagId name) refs 
                    Get   -> return $ GetTag (TagId name) 
                    Alloc -> return $ AddBlob name 3
       call masta rpc :: Process MasterResponse
       return ()

remotable ['testClient]

main :: Backend -> NodeId -> [NodeId] -> [NodeId] -> Process ()
main be mnode slaveNodes clientNodes
  = do moi  <- getSelfPid
       spawnSymmetric clientNodes $ $(mkBriskClosure 'testClient) moi
       runMaster be slaveNodes
