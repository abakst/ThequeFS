{-# LANGUAGE DeriveDataTypeable #-}
module Theque.Thequefs.CmdLine where
import System.Console.CmdArgs

----------------------------------------------------------
-- Command Line Modes
----------------------------------------------------------
data ThequeFS = Master  { host :: String, port :: String }
              | Slave   { host :: String, port :: String }
              | NewBlob { master      :: String
                        , blobName    :: String
                        }
              | PushData { node     :: String
                         , blobId   :: String
                         , blobData :: String
                         }
              deriving (Show, Data, Typeable)

getCmdArgs :: IO ThequeFS
getCmdArgs = cmdArgs thequeArgs


thequeArgs :: ThequeFS
thequeArgs = modes [ master   &= name "master"
                   , slave    &= name "slave"
                   , newblob  &= name "newblob"
                   , pushblob &= name "push"
                   ]
  where
    master  = Master { host = "localhost" &= help "Host to run master on"
                     , port = "9001" &= help "Port to run master on"
                     }
    slave   = Slave  { host = "localhost" &= help "Host to run slave on"
                     , port = "9002" &= help "Port to run slave on"
                     }
    newblob = NewBlob { master      = "localhost:9001"
                      , blobName    = def &= help "Blob Name Prefix"
                      }
    pushblob = PushData { node     = def &= help "Data Node Address"
                        , blobId   = def &= help "Allocated Blob ID"
                        , blobData = def &= help "Blob Data"
                        }
