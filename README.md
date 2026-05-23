# PyTransfer
funky file transfer thing for my grade 12 computer science final
inspired by [Magic Wormhole](https://github.com/magic-wormhole/magic-wormhole)

NOT secure at all please dont actually use this for anything important😭


# usage
1. clone the repository
2. `pip3 install -r requirements.txt`
3. you'll need to edit the server bind address in line 143 of `server.py` if you intend on using this on seprate machines
4. you'll also need to put the same address in line 11 of ```___main___.py```
5. run the server in `./server/` with `python3 server.py`
6. run the clients in `./client/ with ```python3 ___main___.py```
