{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
module Theque.Thequefs.Types
  ( BlobId
  , BlobLoc(..)
  , Blob(..)
  , TagId(..)
  , SpecialTag(..)
  , TagRef(..)
  , Tag(..)
  , endPoint
  , findService
  , makeNodeId
  ) where

import Data.Aeson
import Data.Hashable
import GHC.Generics (Generic)
import Data.Binary
import Data.Data
import Data.Typeable
import Network.Transport
import qualified Data.ByteString.Char8 as BS

import Control.Distributed.Process.Extras (resolve)
import Control.Distributed.Process

----------------------------------------------------------
-- Core Types for ThequeFS Processes
----------------------------------------------------------
type BlobId = String
data BlobLoc = BlobLoc { blobEndPoint :: EndPointAddress
                       , blobId       :: BlobId
                       }
               deriving (Eq, Ord, Show, Generic)
instance Binary BlobLoc
instance ToJSON EndPointAddress where
  toJSON e = toJSON (show e)
instance ToJSON BlobLoc 

data Blob    = Blob { blobURL  :: BlobLoc
                    , blobData :: BS.ByteString
                    }
          deriving (Eq, Ord, Show, Generic)
instance Binary Blob

data TagId = TagId String
           | SpecialTagId SpecialTag
           deriving (Eq, Ord, Show, Generic)
instance Binary TagId
instance Hashable TagId where
  hashWithSalt s (TagId i)
    = hashWithSalt s i
  hashWithSalt s (SpecialTagId Delete)
    = hashWithSalt s "+delete+"

data SpecialTag = Delete
                deriving (Eq, Ord, Show, Generic)
instance Binary SpecialTag

data TagRef = OtherTag TagId
            | Blobs    [Blob]
            deriving (Eq, Ord, Show, Generic)

data Tag = Tag { tagId   :: TagId
               , tagRefs :: [TagRef]
               , tagRev  :: Int
               }
         deriving (Eq, Ord, Show, Generic)
instance Binary TagRef
instance Binary Tag

endPoint :: ProcessId -> EndPointAddress
endPoint = nodeAddress . processNodeId

makeNodeId :: String -> NodeId
makeNodeId addr
  = NodeId
  . EndPointAddress
  $ BS.pack addr

findService :: NodeId -> String -> Process ProcessId
findService srv service
  = do say (show srv ++ " " ++ service)
       Just s <- resolve (srv, service)
       return s
