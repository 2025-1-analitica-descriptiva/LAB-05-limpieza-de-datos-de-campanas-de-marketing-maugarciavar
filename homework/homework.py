"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel
import pandas as pd
import zipfile
import os

def mes_a_numero(nombre_mes):
    meses = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4,
        "may": 5, "jun": 6, "jul": 7, "aug": 8,
        "sep": 9, "oct": 10, "nov": 11, "dec": 12,
    }
    return meses.get(nombre_mes.lower(), 0)

def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months
    """

    carpeta_entrada = "files/input"
    carpeta_salida = "files/output"
    os.makedirs(carpeta_salida, exist_ok=True)

    # Leer y concatenar los CSV dentro de ZIPs
    lista_dataframes = []
    for archivo_zip in os.listdir(carpeta_entrada):
        if archivo_zip.endswith(".zip"):
            ruta_zip = os.path.join(carpeta_entrada, archivo_zip)
            with zipfile.ZipFile(ruta_zip, "r") as zip_obj:
                for nombre_csv in zip_obj.namelist():
                    if nombre_csv.endswith(".csv"):
                        with zip_obj.open(nombre_csv) as archivo_csv:
                            df_temp = pd.read_csv(archivo_csv, sep=",")
                            lista_dataframes.append(df_temp)

    df_completo = pd.concat(lista_dataframes, ignore_index=True)

    # -- client.csv --
    df_clientes = df_completo[
        ["client_id", "age", "job", "marital", "education", "credit_default", "mortgage"]
    ].copy()

    df_clientes["job"] = (
        df_clientes["job"]
        .str.replace(".", "", regex=False)
        .str.replace("-", "_", regex=False)
    )
    df_clientes["education"] = df_clientes["education"].str.replace(".", "_", regex=False)
    df_clientes["education"] = df_clientes["education"].replace("unknown", pd.NA)
    df_clientes["credit_default"] = df_clientes["credit_default"].apply(
        lambda valor: 1 if str(valor).lower() == "yes" else 0
    )
    df_clientes["mortgage"] = df_clientes["mortgage"].apply(
        lambda valor: 1 if str(valor).lower() == "yes" else 0
    )
    df_clientes.to_csv(os.path.join(carpeta_salida, "client.csv"), index=False)

    # -- campaign.csv --
    df_campaña = df_completo[
        [
            "client_id", "number_contacts", "contact_duration",
            "previous_campaign_contacts", "previous_outcome",
            "campaign_outcome", "month", "day"
        ]
    ].copy()

    df_campaña["previous_outcome"] = df_campaña["previous_outcome"].apply(
        lambda valor: 1 if str(valor).lower() == "success" else 0
    )
    df_campaña["campaign_outcome"] = df_campaña["campaign_outcome"].apply(
        lambda valor: 1 if str(valor).lower() == "yes" else 0
    )

    df_campaña["last_contact_date"] = df_campaña.apply(
        lambda fila: f"2022-{mes_a_numero(fila['month']):02d}-{int(fila['day']):02d}",
        axis=1
    )
    df_campaña = df_campaña.drop(columns=["month", "day"])
    df_campaña.to_csv(os.path.join(carpeta_salida, "campaign.csv"), index=False)

    # -- economics.csv --
    df_economia = df_completo[
        ["client_id", "cons_price_idx", "euribor_three_months"]
    ].copy()
    df_economia.to_csv(os.path.join(carpeta_salida, "economics.csv"), index=False)


if __name__ == "__main__":
    clean_campaign_data()