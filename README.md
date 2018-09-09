# PySpark: Friend Recommendation

A simple PySpark program that recommends new friends to individual users based on their mutual friends. Only the top 10 recommendations are calculated, and they are in order from most recommended to least recommended.

## File Requirements

You must have a friend network file handy. Each line of the file should start with a user ID (integer), follwed by a comma-separated list of user IDs that are friends with the user ID at the beginning of the line. For example:

```
0   1,2,3
1   0,2,4,5,6
2   0,1,4,6,7,8
3   0,4,7
4   1,2,3,5,8
5   1,4
6   1,2
7   2,3
8   2,4
```

Note that every friendship (e.g. 0->2) is matched in reverse (e.g. 2->0) by both friends.

## Execute

Have a friend network file handy. To execute the program, you do either of the following:

```bash
spark-submit friend-recommendation NETWORK_FILE OUTPUT_DIRECTORY
```

OR

```bash
chmod +x run-spark.sh # only need to do this once
./run-spark.sh TEXT_FILE OUTPUT_DIRECTORY
```

## Memory Issues

If your network file is sufficiently large, you may need to allocate more RAM to your nodes. Luckily, the `run-spark.sh` script in this repo already allocated 8GB of RAM to each node, but if you are executing this script manually (i.e. the first option in the `Execute` section), just run the following:

```bash
spark-submit --driver-memory 8G friend-recommendation.py NETWORK_FILE OUTPUT_DIRECTORY
```

## Output

The output will come in `part-*` files in the output directory of your choosing.
Each line in a `part-*` file will contain a string in the following format:

```
(USER_ID, [REC_0, REC_1, ... REC_9])
```

For example:
```
(922, [916, 915, 919, 923, 39527, 41346, 857, 912, 914, 917])
```

There should be exactly one key for every user ID present in your network file amongst all part files.
