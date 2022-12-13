from __future__ import annotations

import datetime 
from datetime import timedelta
import pendulum
import pandas as pd
import numpy as np

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator

from typing import Iterable

import jaydebeapi
from   jaydebeapi import Error

#Se define funcion para escribir info en csv
@task(task_id="to_csv")
def to_csv(informacion):
    csv = open(f"/home/jaime/Downloads/{informacion['DAG']}.csv","a")
    contenido = f"{informacion['DAG']},{informacion['Tabla']},{pendulum.now('America/Mazatlan').format('YYYY-MM-DD HH:mm:ss')}\r\n"
    csv.write(contenido)
    return 0