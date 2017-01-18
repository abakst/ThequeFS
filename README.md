## ThequeFS 

## ThequeFS

A cluster file system re-implementation of the Disco cluster file system. 

For Great Justice.

## Usage

thequefs [COMMAND] ... [OPTIONS]

Common flags:
  -? --help           Display help message
  -V --version        Print version information

thequefs master [OPTIONS]

  -h --host=ITEM      Host to run master on
  -p --port=ITEM      Port to run master on

thequefs slave [OPTIONS]

  -h --host=ITEM      Host to run slave on
  -p --port=ITEM      Port to run slave on

thequefs newblob [OPTIONS]

  -m --master=ITEM  
  -b --blobname=ITEM  Blob Name Prefix

thequefs push [OPTIONS]

  -n --node=ITEM      Data Node Address
     --blobid=ITEM    Allocated Blob ID
     --blobdata=ITEM  Blob Data
