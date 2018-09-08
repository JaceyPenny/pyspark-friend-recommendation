import sys
import itertools
from pyspark import SparkConf, SparkContext


def line_to_friend_ownership(line):
    """
    Parses a line from a social network text file into a "friend ownership" structure.

    For example, the line: ``0    1,2,3,4`` will get parsed to:

    ``(0, [1, 2, 3, 4])``

    That is, a python tuple where the first element is an int and the second element is a List[int]

    :param line: str
    :return: Tuple[int, List[int]], the parsed line
    """
    split = line.split()
    user_id = int(split[0])

    if len(split) == 1:
        friends = []
    else:
        friends = list(map(lambda x: int(x), split[1].split(',')))

    return user_id, friends


def friend_ownership_to_connection(f_o):
    """
    Maps a "friend ownership" structure (see the above method) to an array of connections.

    For example, the value ``(0, [1, 2, 3])`` will get mapped to::

        [
            ((0,1), 0),
            ((0,2), 0),
            ((0,3), 0),
            ((1,2), 1),
            ((1,3), 1),
            ((2,3), 1)
        ]

    Essentially, the friend ownership structure is converted a list of all connection information embedded in the
    structure. For example, users 0 and 1 are already connected, so that connection is represented by ``((0,1), 0)``.
    The final ``0`` indicates that these users are currently friends.

    As another example, the structure encodes the fact that users 1 and 2 have a mutual friend (in this case, the mutual
    friend is friend 0). So, the resulting connection is represented by ``((1,2), 1)``, where the ``1`` indicates that
    these users share a single mutual friend.

    Finally, it is important to note that the "key" in each of these elements (namely the ``(user_id_0, user_id_1)``
    pair) is deterministically ordered. It is important for each unique pair of users to be grouped in the same way,
    so the bi-directional relationship must be retained by ordering the tuple by userId (or any other deterministic
    ordering. Simple "greater-than" comparison just happens to be the fastest).

    :param f_o: the friendship ownership object
    :return: List[Tuple[Tuple[int, int], int]] the embedded connections
    """
    user_id = f_o[0]
    friends = f_o[1]

    connections = []

    for friend_id in friends:
        key = (user_id, friend_id)
        if user_id > friend_id:
            key = (friend_id, user_id)

        connections.append(
            (key, 0)
        )

    for friend_pair in itertools.combinations(friends, 2):
        friend_0 = friend_pair[0]
        friend_1 = friend_pair[1]

        key = (friend_0, friend_1)
        if friend_0 > friend_1:
            key = (friend_1, friend_0)
        connections.append(
            (key, 1)
        )

    return connections


def mutual_friend_count_to_recommendation(m):
    """
    Maps a "mutual friend count" object to two distinct recommendations. The value
    ``((0, 1), 21)`` encodes that users 0 and 1 share 21 mutual friends. This means that user 1 should be recommended
    to user 0 AND that user 0 should be recommended to user 1. For every input to this function, two "recommendations"
    will be returned in a List.

    A "recommendation" has the following form::

        (user_id_0, (recommended_user, mutual_friends_count))

    :param m: a mutual friend count item
    :return: List[Tuple[int, Tuple[int, int]]] two recommendation items
    """
    connection = m[0]
    count = m[1]

    friend_0 = connection[0]
    friend_1 = connection[1]

    recommendation_0 = (friend_0, (friend_1, count))
    recommendation_1 = (friend_1, (friend_0, count))

    return [recommendation_0, recommendation_1]


def recommendation_to_sorted_truncated(recs):
    if len(recs) > 1024:
        # Before sorting, find the highest 10 elements in recs (if log(len(recs)) > 10)
        max_indices = []

        for current_rec_number in range(0, 10):
            current_max_index = 0
            for i in range(0, len(recs)):
                rec = recs[i]
                if rec[1] > recs[current_max_index][1] and i not in max_indices:
                    current_max_index = i

            max_indices.append(current_max_index)

        recs = [recs[i] for i in max_indices]

    recs.sort(key=lambda x: x[1], reverse=True)

    # Map every [(user_id, mutual_count), ...] to [user_id, ...] and truncate to 10 elements
    return list(map(lambda x: x[0], recs))[:10]


# ============ #
# MAIN PROGRAM #
# ============ #

# Initialize spark configuration and context
conf = SparkConf()
sc = SparkContext(conf=conf)

# Read from text file, split each line into "words" by any whitespace (i.e. empty parameters to string.split())
lines = sc.textFile(sys.argv[1])

# Map each line to the form: (user_id, [friend_id_0, friend_id_1, ...])
friend_ownership = lines.map(line_to_friend_ownership)

# Map each "friend ownership" to multiple instances of ((user_id, friend_id), VALUE).
# VALUE = 0 indicates that user_id and friend_id are already friends.
# VALUE = 1 indicates that user_id and friend_id are not friends.
friend_edges = friend_ownership.flatMap(friend_ownership_to_connection)
friend_edges.cache()

mutual_friend_counts = friend_edges.groupByKey() \
    .filter(lambda edge: 0 not in edge[1]) \
    .map(lambda edge: (edge[0], sum(edge[1])))

recommendations = mutual_friend_counts.flatMap(mutual_friend_count_to_recommendation) \
    .groupByKey() \
    .map(lambda m: (m[0], recommendation_to_sorted_truncated(list(m[1]))))

# Save to output directory, end context
recommendations.saveAsTextFile(sys.argv[2])
sc.stop()
