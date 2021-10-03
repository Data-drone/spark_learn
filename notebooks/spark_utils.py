from pyspark.sql import SparkSession
import os, pwd, socket

def get_k8s_spark():
    
    """
    
    Spark common configs for launching session in:
    - data_eng_kube setup
    - jupyterhub session
    - executor pods in jhub namespace
    
    """
    
    driver_host = socket.gethostbyname(socket.gethostname())
    username = pwd.getpwuid( os.getuid() )[ 0 ]
    
    spark = (SparkSession
                 .builder
                 .config("spark.master", "k8s://https://kubernetes.default.svc.cluster.local:443")
                 .config("spark.submit.deployMode", "client")
                 .config("spark.kubernetes.authenticate.driver.serviceAccountName", "spark")
                 .config("spark.kubernetes.authenticate.serviceAccountName", "spark")
                 .config("spark.kubernetes.namespace", "jhub")
                 .config("spark.kubernetes.driver.pod.name", "jupyter-{0}".format(username))
                 .config("spark.driver.host", driver_host)
                 .config("spark.driver.bindAddress", "0.0.0.0")
                 .config("spark.driver.port", "7777")
                 .config("spark.driver.blockManager.port", "2222")
                 .config("spark.kubernetes.executor.label.app", "jupyterhub")
                 .config("spark.kubernetes.executor.label.heritage", "jupyterhub")
                 .config("spark.kubernetes.executor.label.hub.jupyter.org/username", username)
            )
    
    return spark