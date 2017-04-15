#define CHAN(_name,_ty) _name##_ty
#define DECLARE_CHAN(_name,_sz,_ty) chan CHAN(_name,_ty)[_sz] = [2] of { _ty }
#define RECVLOOP(_idx, _chan, _msg) \
  _chan[_idx]?[_msg] -> _chan[_idx]?_msg
