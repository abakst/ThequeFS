#include "Common/defs.pml"

mtype {
    AddBlob
  , GetBlob
  , AddTag
  , GetTag
  , GetTagInfo
  , SetTag
  , OK
  , Blob
  , BlobNotFound
  , TagFound
  , TagInfo
  , TagNotFound
  , BlobName
  , PushBlob
}

typedef MasterAPI {
  mtype tag;
  pid from;
  int id; /* Overloaded */
  int dat;
};
typedef MasterResponse {
  mtype tag;
  pid from;
};

typedef TagNodeAPI {
  mtype tag;
  pid from;
  int id;
  int dat;
};

typedef DataNodeAPI {
  mtype tag;
  pid from;
  int id;
  int dat;
};

typedef TagNodeResponse {
  mtype tag;
  pid from;
};
typedef DataNodeResponse {
  mtype tag;
  pid from;
};

DECLARE_CHAN(master_tagnode,N,TagNodeAPI);
DECLARE_CHAN(tagnode_master,N,TagNodeResponse);

DECLARE_CHAN(client_datanode,N,DataNodeAPI);
DECLARE_CHAN(datanode_client,N,DataNodeResponse);

DECLARE_CHAN(mclient_master,N,MasterAPI);
DECLARE_CHAN(master_mclient,N,MasterResponse);
proctype tagnode(int off)
{
  pid me = _pid - off;
  TagNodeAPI msg;
  TagNodeResponse resp;
  resp.from = me;

end:
  do
    :: CHAN(master_tagnode,TagNodeAPI)[me]?msg;
       if
         :: msg.tag == SetTag ->
            resp.tag  = OK;
            
         :: msg.tag == GetTag ->
            if
              :: resp.tag = TagFound;
              :: resp.tag = TagNotFound;
            fi
            
         :: msg.tag == GetTagInfo;
            if
              :: resp.tag = TagInfo;
              :: resp.tag = TagNotFound;
            fi
       fi;
       CHAN(tagnode_master,TagNodeResponse)[me]!resp;
  od
}
proctype datanode(int off)
{
  pid me = _pid - off;
  DataNodeAPI msg;
  DataNodeResponse resp;
  resp.from = me;

end:
  do
    :: CHAN(client_datanode,DataNodeAPI)[me]?msg;
       if
         :: msg.tag == GetBlob;
            if
              :: resp.tag = Blob;
              :: resp.tag = BlobNotFound
            fi
         :: msg.tag == PushBlob;
            resp.tag = OK;
       fi
       CHAN(datanode_client,DataNodeResponse)[me]!resp;
  od
}
proctype master(int off)
{
  pid me = _pid - off;
  MasterAPI msg;
  MasterResponse resp;
  TagNodeAPI tagmsg;
  TagNodeResponse tagresp;
  resp.from = me;
  int idx;

end:
  do
    ::
       __RECVLOOP(0,CHAN(mclient_master,MasterAPI),msg);
       if
         :: msg.tag == AddBlob ->
            resp.tag = BlobName;

         :: msg.tag == AddTag ->
            /* get most up to date tag */
            for (idx : 0 .. (N-1) ) {
              tagmsg.tag  = GetTag;
              tagmsg.from = me;
              CHAN(master_tagnode,TagNodeAPI)[idx]!tagmsg;
              CHAN(tagnode_master,TagNodeResponse)[idx]?tagresp;
            }

            /* set new tag */
            for (idx : 0 .. (N-1) ) {
              tagmsg.tag  = SetTag;
              tagmsg.from = me;
              CHAN(master_tagnode,TagNodeAPI)[idx]!tagmsg;
              CHAN(tagnode_master,TagNodeResponse)[idx]?tagresp;
            }

            resp.tag = OK;

         :: msg.tag == GetTag ->
            /* get most up to date tag */
            for (idx : 0 .. (N-1) ) {
              tagmsg.tag  = GetTag;
              tagmsg.from = me;
              CHAN(master_tagnode,TagNodeAPI)[idx]!tagmsg;
              CHAN(tagnode_master,TagNodeResponse)[idx]?tagresp;
            }
            if
              :: resp.tag = TagNotFound;
              :: resp.tag = TagFound;
            fi         
       fi;
       CHAN(master_mclient,MasterResponse)[msg.from]!resp;
  od
}
proctype mclient(int off)
{
  pid me = _pid - off;
  MasterAPI msg;
  MasterResponse resp;

  msg.from = me;
  if
    :: msg.tag = AddBlob;
    :: msg.tag = GetTag;
    :: msg.tag = AddTag;
  fi;

  CHAN(mclient_master,MasterAPI)[me]!msg;
  CHAN(master_mclient,MasterResponse)[me]?resp;
}
proctype client(int off)
{
  pid me = _pid - off;
  DataNodeAPI msg;
  DataNodeResponse resp;
  int idx;

  msg.from = me;
  if
    :: msg.tag = GetBlob;
    :: msg.tag = PushBlob;
  fi;

  select ( idx : 1 .. N );
  idx = idx - 1;
  CHAN(client_datanode,DataNodeAPI)[idx]!msg;
  CHAN(datanode_client,DataNodeResponse)[idx]?resp;
}

init {
  atomic {
  int i;
  int x;
  x = _nr_pr;
  run client(x);
  x = _nr_pr;
  for (i : 1 .. N ) {
    run mclient(x);
  }
  x = _nr_pr;
  run master(x);
  x = _nr_pr;
  for (i : 1 .. N ) {
    run datanode(x);
  }
  x = _nr_pr;
  for (i : 1 .. N ) {
    run tagnode(x);
  }
  }
}