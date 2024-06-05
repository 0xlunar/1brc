# 1brc-rs
1 Billion row challenge 

Runs in ~9seconds on my machine, can be improved with better file reading and maybe more optimised parsing,
but I'm relatively happy with the results I have currently.
```
    CPU: Ryzen 9 7950x
    MEMORY: 32GB DDR5 5600MHz
```

### Potential Bug?
Not sure if my dataset is bugged or my parsing is bugged but the results mostly end up being `-99.9;0.0;99.9` 
for most of the stations, I'm sure this is just due to the small float range and limited number of stations for 
1 billion rows.