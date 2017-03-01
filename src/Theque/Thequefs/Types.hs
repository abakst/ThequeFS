{-# LANGUAGE DeriveDataTypeable #-}
{-# LANGUAGE DeriveGeneric #-}
{-#
  OPTIONS_GHC -fplugin      Brisk.Plugin
              -fplugin-opt  Brisk.Plugin:runDataNode
#-}
module Theque.Thequefs.Types
  ( BlobId
  , BlobLoc(..)
  , Blob(..)
  , TagId(..)
  , SpecialTag(..)
  , TagRef(..)
  , Tag(..)
  , blobLoc
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
import Data.List.Split
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
instance ToJSON TagId
instance Hashable TagId where
  hashWithSalt s (TagId i)
    = hashWithSalt s i
  hashWithSalt s (SpecialTagId Delete)
    = hashWithSalt s "+delete+"

data SpecialTag = Delete
                deriving (Eq, Ord, Show, Generic)
instance Binary SpecialTag
instance ToJSON SpecialTag

data TagRef = OtherTag TagId
            | Blobs    [BlobLoc]
            deriving (Eq, Ord, Show, Generic)

data Tag = Tag { tagId   :: TagId
               , tagRefs :: [TagRef]
               , tagRev  :: Int
               }
         deriving (Eq, Ord, Show, Generic)
instance Binary TagRef
instance Binary Tag
instance ToJSON TagRef
instance ToJSON Tag

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

blobLoc :: String -> BlobLoc
blobLoc s
  = BlobLoc { blobEndPoint = EndPointAddress bloburl
            , blobId       = bid
            }
  where
    splits  = splitOn ":" s
    bloburl = BS.intercalate (BS.pack ":") (BS.pack <$> init splits)
    bid     = last splits
