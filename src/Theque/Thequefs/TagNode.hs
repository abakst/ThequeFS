{-# LANGUAGE DeriveGeneric #-}
module Theque.Thequefs.TagNode where

import Data.List
import Data.Binary
import GHC.Generics (Generic)
import qualified Data.ByteString as BS
import qualified Data.HashMap.Strict as M
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Extras.Time
import Control.Distributed.Process.ManagedProcess

import Theque.Thequefs.Types hiding (master)

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

data TagNodeState = DNS {
    master :: ProcessId
  , tags   :: !TagMap
  }

data TagNodeAPI = GetTag TagId
                | GetTagInfo TagId
                | AddTag TagId [TagRef]
                deriving (Eq, Ord, Show, Generic)
instance Binary TagNodeAPI

data TagNodeResponse = OK
                     | TagInfo Int   -- ^ Version of requested tag
                     | TagFound Tag
                     | TagNotFound
                      deriving (Eq, Ord, Show, Generic)
instance Binary TagNodeResponse

initState m = DNS { tags = M.empty, master = m }

runTagNode :: ProcessId -> Process ()
runTagNode m =
  serve (initState m) initializeDataNode tagNodeProcess

initializeDataNode s = return $ InitOk s NoDelay

tagNodeProcess :: ProcessDefinition TagNodeState
tagNodeProcess = defaultProcess {
  apiHandlers = [tagNodeAPIHandler]
  }

type TagNodeReply = Process (ProcessReply TagNodeResponse TagNodeState)

tagNodeAPIHandler :: Dispatcher TagNodeState
tagNodeAPIHandler = handleCall tagNodeAPIHandler'

tagNodeAPIHandler' :: TagNodeState -> TagNodeAPI -> TagNodeReply
tagNodeAPIHandler' s (GetTag tid)
  = reply response s
  where
    response = maybe TagNotFound TagFound $ lookupTag s tid
tagNodeAPIHandler' s (GetTagInfo tid)
  = reply response s
  where
    response = maybe TagNotFound TagInfo
             . fmap tagRev
             $ lookupTag s tid
tagNodeAPIHandler' s (AddTag tid refs)
  = reply response s'
  where
    s'       = addTag s tid refs
    response = OK

lookupTag :: TagNodeState -> TagId -> Maybe Tag
lookupTag s id = M.lookup id (tags s)

addTag :: TagNodeState -> TagId -> [TagRef] -> TagNodeState  
addTag s tid refs
  = s { tags = M.insert tid newTag (tags s) }
  where
    oldTag = M.lookupDefault (emptyTag tid) tid (tags s)
    newTag = oldTag { tagRefs = nub (tagRefs oldTag ++ refs) }

emptyTag :: TagId -> Tag    
emptyTag tid = Tag { tagId   = tid
                   , tagRefs = []
                   , tagRev  = 0
                   }
