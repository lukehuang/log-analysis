# location of Apache Spark locally
SPARK_HOME = '/usr/local/Cellar/apache-spark/2.1.0/libexec/'

# main log file
LOG_FILE = 'access.log'

# savepoint file location
SAVEPOINT = 'save.tmp'

# Kafka server
KAFKA_SERVERS = 'localhost:9092'

# Kafka topic
KAFKA_TOPIC = 'test'

# Kafka results topic
KAFKA_RESULTS_TOPIC = 'results'

try:
    from local_settings import *
except ImportError as e:
    pass