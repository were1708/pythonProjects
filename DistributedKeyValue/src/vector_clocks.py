import globals



def add_key(self: dict, key: str) -> None:
        self[key] = [0] * len(globals.current_view)
        return 

    # sets entire clock (all keys) to another clock (all keys)
def copy(self: dict, other_clock_all: dict) -> None:
        self = other_clock_all
        return self
    
    # sets the clock at key to the clock passed in
def copy_key(self: dict, key: str, other_clock: list) -> None:
        self[key] = other_clock
        return

def reset (self: dict) -> None: # clears the clock! 
        self.clear()
        return

def increment(self: dict, key: str, index: int ) -> None: # index is the index of your number in the VC
        self[key][index] += 1
        return

def combine(self: dict, key: str, other_clock: list) -> None:
        if self.get(key) is None:
              self[key] = other_clock
              return
        if not other_clock:
              return
        
        if len(self[key]) > len(other_clock):
            other_clock.extend([0] * (len(self[key]) - len(other_clock)))
        elif len(self[key]) < len(other_clock):
              self[key].extend([0] * (len(other_clock) - len(self[key])))

              
        for index in range(len(self[key])):
            self[key][index] = max(self[key][index], other_clock[index])
        return
    
    # returns the value of the whole clock.
    # primarily used for catching a node up!
def get_clock(self: dict) -> dict:
        return self
    
    # returns the clock for one key
def get_key_clock(self: dict, key: str) -> list:
        return self[key]


    #                       PLEASE NOTE:
    #   this function does not take in another Vector_Clock object
    #   when using this function please pass the list (the actual clock)
    #   of clock you would like to compare it to. I made it this way
    #   because when we recieve a clock from a message, we don't 
    #   wanna have to package it into a object. instead we can just 
    #   use the list that was given to us in the message.
def compare(self: dict, key: str, other_clock: list)  -> int:
    # function compares the vector clocks of two clocks
    # input: list representing the clock of another,
    #        a key for the clock you wish to compare
    #
    # output: 1 if self is greater than other clock,
    #         0 if concurrent
    #         -1 if self is less than other clock
    #         2 if equal

        GREATER_THAN = 1
        LESS_THAN = -1
        CONCURRENT = 0
        EQUAL_TO = 2

        
        less_val_found = False
        greater_val_found = False



        clock_check = self.get(key, [0] * len(globals.current_view))

        if not clock_check and not other_clock: # both clocks are empty
            return EQUAL_TO
        elif not other_clock: # only other_clock is empty
            return GREATER_THAN
        elif not clock_check: # our clock is empty
              return LESS_THAN


        if len(clock_check) > len(other_clock):
              other_clock.extend([0] * (len(clock_check) - len(other_clock)))
        elif len(clock_check) < len(other_clock):
              clock_check.extend([0] * (len(other_clock) - len(clock_check)))


        for i in range(len(clock_check)):
            if clock_check[i] > other_clock[i]:
                greater_val_found = True
            elif clock_check[i] < other_clock[i]:
                less_val_found = True
            else:
                continue
        
        if greater_val_found and not less_val_found:
            return GREATER_THAN
        elif less_val_found and not greater_val_found:
            return LESS_THAN
        elif less_val_found and greater_val_found:
            return CONCURRENT
        else:
            return EQUAL_TO

def update_known_clocks(causal_metadata: dict):
    if causal_metadata is None:
          return
    for key, clock in causal_metadata.items():
        if key not in globals.known_clocks:
            globals.known_clocks[key] = clock
        else:
            combine(globals.known_clocks, key, clock)