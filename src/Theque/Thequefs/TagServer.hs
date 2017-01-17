{-# LANGUAGE TemplateHaskell #-}
{-# LANGUAGE DeriveGeneric #-}
module Theque.Thequefs.TagServer where

import Data.Binary
import GHC.Generics (Generic)
import qualified Data.ByteString as BS
import qualified Data.HashMap.Strict as M
import Control.Distributed.Process hiding (call)
import Control.Distributed.Process.Extras.Time
import Control.Distributed.Process.ManagedProcess

-- type TagId  = String
-- data Tagged = TaggedBlob BlobId
-- type TagMap = M.HashMap TagId Tagged

-- data TagServerState = TS {
--   master :: ProcessId
--   -- tags ::
--   }
