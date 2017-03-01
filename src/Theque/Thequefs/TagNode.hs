{-# LANGUAGE DeriveGeneric #-}
{-#
  OPTIONS_GHC -fplugin      Brisk.Plugin
              -fplugin-opt  Brisk.Plugin:runTagNode
#-}
module Theque.Thequefs.TagNode (runTagNode, setTag, getTag, TagNodeResponse(..)) where

import           Data.List
import           Data.Binary
import           GHC.Generics (Generic)
import qualified Data.ByteString as BS
import qualified Data.HashMap.Strict as M
import           Control.Distributed.Process hiding (call)
import           Control.Distributed.Process.Extras.Time
import           Control.Distributed.Process.ManagedProcess

import           GHC.Base.Brisk
import           Control.Distributed.Process.Brisk hiding (call)
import           Control.Exception.Base.Brisk                 
import           Control.Distributed.Process.ManagedProcess.Brisk hiding (call)

import           Theque.Thequefs.Types hiding (master)

{-
DataNode:
1. register datanode service
2. start "space reporter" ???
3. start RPC service
-}
tagNodeService :: String
tagNodeService = "theque:tagNode"

type BlobId = String
type TagMap = M.HashMap TagId Tag

data TagNodeState = TagState {
    tags   :: !TagMap
  }
initState = TagState { tags = M.empty }

data TagNodeAPI = GetTag TagId
                | GetTagInfo TagId
                | SetTag TagId Tag
                deriving (Eq, Ord, Show, Generic)
instance Binary TagNodeAPI

getTag :: ProcessId -> TagId -> Process TagNodeResponse         
getTag tn tid = call tn (GetTag tid)

setTag :: ProcessId -> TagId -> Tag -> Process TagNodeResponse         
setTag tn tid tag = call tn (SetTag tid tag)

data TagNodeResponse = OK
                     | TagInfo Int   -- ^ Version of requested tag
                     | TagFound Tag
                     | TagNotFound
                      deriving (Eq, Ord, Show, Generic)
instance Binary TagNodeResponse


{-# NOINLINE runTagNode #-}
runTagNode :: Process ()
runTagNode = serve initState initializeTagNode tagNodeProcess

initializeTagNode :: s -> Process (InitResult s)
initializeTagNode s = return $ InitOk s NoDelay

{-# NOINLINE tagNodeProcess #-}
tagNodeProcess :: ProcessDefinition TagNodeState
tagNodeProcess = defaultProcess {
  apiHandlers = tagNodeHandlers 
  }

tagNodeHandlers = [tagNodeAPIHandler]  

type TagNodeReply = Process (ProcessReply TagNodeResponse TagNodeState)

{-# NOINLINE tagNodeAPIHandler #-}
tagNodeAPIHandler :: Dispatcher TagNodeState
tagNodeAPIHandler = handleCall tagNodeAPIHandler'

mkTgFound x = TagFound x

tagNodeAPIHandler' :: TagNodeState -> TagNodeAPI -> TagNodeReply
tagNodeAPIHandler' s (GetTag tid)
  = reply response s
  where
    response = maybe TagNotFound mkTgFound $ lookupTag s tid
tagNodeAPIHandler' s (GetTagInfo tid)
  = reply response s
  where
    response = maybe TagNotFound TagInfo
             . fmap tagRev
             $ lookupTag s tid
tagNodeAPIHandler' s (SetTag tid tag)
  = reply response s'
  where
    s'       = doSetTag s tid tag
    response = OK

lookupTag :: TagNodeState -> TagId -> Maybe Tag
lookupTag s id = M.lookup id (tags s)

doSetTag :: TagNodeState -> TagId -> Tag -> TagNodeState
doSetTag s tid tag
  = s { tags = M.insert tid tag (tags s) }

emptyTag :: TagId -> Tag    
emptyTag tid = Tag { tagId   = tid
                   , tagRefs = []
                   , tagRev  = 0
                   }
