import sys
import os
import pandas as pd
import logging
import smtplib
from itertools import combinations
from functools import reduce
from email.message import EmailMessage
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError, OperationalError

FETCH_WINDOW_MINUTES = 15000

DATA_INIZIO = '2025-10-01 00:00:00'
DATA_FINE = '2027-07-03 00:00:00'

SUM_THRESHOLD = 1
OCCURRENCE_THRESHOLD = 2
OCCULT_THRESHOLDS = [4, 5, 6]
QUOTA_EXTRA_THRESHOLD = 108

OUTPUT_DIR = "output"

SIMILARITY_MINIMA_UTENTE = 80.0
CSV_SEPARATOR = ";"
CSV_DECIMAL = ","

try:
    pd.set_option("future.no_silent_downcasting", True)
except Exception:
    pass

SCOMMESSE_FOCUS = [
    1,
    9899, 9900,
    12447, 12547,
    13353, 13439,
    13604, 13605, 13656, 13657, 13658, 13661,
    14176, 14474, 14476, 14477, 14478,
    14510, 14514, 14515,
    14613, 14762, 14815, 15173,
    16035, 16177, 16190, 16474,
    17016, 17152, 17589, 17637,
    18568, 18694, 18696,
    19804,
    20138, 20145, 20148, 20160, 20162,
    20202, 20207, 20392,
    21073, 21115, 21147, 21453, 21777,
    22580, 23423,
    24480, 24481, 24482, 24483, 24484, 24485, 24486, 24487,
    24511, 24512, 24513, 24514, 24516, 24518, 24519,
    24521, 24522, 24526, 24552, 24556,
    24904, 25306,
    26190, 26191, 26192,
    26435, 26437, 26441, 26443, 26445, 26446,
    26450, 26454, 26465, 26466, 26468,
    29707, 31499
]

EMAIL_SENDER = os.getenv("EMAIL_SENDER")
EMAIL_PASSWORD = os.getenv("EMAIL_PASSWORD")
EMAIL_RECEIVERS = [
    email.strip()
    for email in os.getenv("EMAIL_RECEIVERS", "").split(",")
    if email.strip()
]

EMAIL_SMTP_SERVER = "smtp.gmail.com"
EMAIL_SMTP_PORT = 587

DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "3306")

REQUIRED_ENV_VARS = {
    "EMAIL_SENDER": EMAIL_SENDER,
    "EMAIL_PASSWORD": EMAIL_PASSWORD,
    "EMAIL_RECEIVERS": EMAIL_RECEIVERS,
    "DB_USER": DB_USER,
    "DB_PASSWORD": DB_PASSWORD,
    "DB_HOST": DB_HOST,
    "DB_PORT": DB_PORT,
}

missing_vars = [name for name, value in REQUIRED_ENV_VARS.items() if not value]

if missing_vars:
    raise ValueError(
        "Variabili ambiente mancanti. Crea questi GitHub Secrets: "
        + ", ".join(missing_vars)
    )

DB_PORT = int(DB_PORT)

DB_CONFIGS = {
    "360BET": {"database": "AnalisiTickets_360BET"},
    "ADMIRAL": {"database": "AnalisiTickets_ADMIRAL"},
    "BBET": {"database": "AnalisiTickets_BBET"},
    "DOMUSBET": {"database": "AnalisiTickets_DOMUSBET"},
    "MARATHON": {"database": "AnalisiTickets_MARATHON"},
    "SKYWIND": {"database": "AnalisiTickets_SKYWIND"},
    "SPORTBET": {"database": "AnalisiTickets_SPORTBET"},
    "STANLEYBET": {"database": "AnalisiTickets_STANLEYBET"},
    "STARCASINO": {"database": "AnalisiTickets_STARCASINO"},
    "TOTOSI": {"database": "AnalisiTickets_TOTOSI"},
    "VINCITU": {"database": "AnalisiTickets_VINCITU"},
    "WILLIAMHILL": {"database": "AnalisiTickets_WILLIAMHILL"},
}

BASE_FILTER_WINDOW = """
WHERE {alias}.num_eventi BETWEEN 2 AND 20
  AND {alias}.ut_ins BETWEEN
      UNIX_TIMESTAMP(DATE_SUB(NOW(), INTERVAL {minutes} MINUTE))
    AND UNIX_TIMESTAMP(NOW())
"""

BASE_FILTER_DATE = """
WHERE {alias}.num_eventi BETWEEN 2 AND 20
  AND STR_TO_DATE({alias}.data_ora_vend, '%Y%m%d %H:%i:%s')
      BETWEEN STR_TO_DATE('{data_inizio}', '%Y-%m-%d %H:%i:%s')
          AND STR_TO_DATE('{data_fine}', '%Y-%m-%d %H:%i:%s')
"""

TUPLE_FIELDS = [
    "cf",
    "palinsesto",
    "manifestazione",
    "avvenimento",
    "scommessa",
    "des_eve",
    "descrizione_info_agg",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)


def salva_csv(df: pd.DataFrame, file_name: str) -> str:
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    file_path = os.path.join(OUTPUT_DIR, file_name)

    df.to_csv(
        file_path,
        index=False,
        sep=CSV_SEPARATOR,
        decimal=CSV_DECIMAL,
        encoding="utf-8-sig"
    )

    logging.info(f"Generato: {file_path}")
    logging.info(f"File presente? {os.path.exists(file_path)}")

    return file_path


def crea_engines():
    engines = {}

    for name, cfg in DB_CONFIGS.items():
        database = cfg["database"]

        url = (
            f"mysql+mysqlconnector://{DB_USER}:{DB_PASSWORD}"
            f"@{DB_HOST}:{DB_PORT}/{database}"
        )

        engines[name] = create_engine(url)

    return engines


def build_filter(alias: str) -> str:
    usa_intervallo_date = (
        DATA_INIZIO is not None
        and DATA_FINE is not None
        and str(DATA_INIZIO).strip() != ""
        and str(DATA_FINE).strip() != ""
    )

    if usa_intervallo_date:
        return BASE_FILTER_DATE.format(
            alias=alias,
            data_inizio=DATA_INIZIO,
            data_fine=DATA_FINE
        )

    return BASE_FILTER_WINDOW.format(
        alias=alias,
        minutes=FETCH_WINDOW_MINUTES
    )


def carica_tickets(engine):
    scommesse_sql = ",".join(str(x) for x in SCOMMESSE_FOCUS)

    where = build_filter("tg") + f"""
      AND tg.importo_pagato > 0
      AND tg.des_stato = 'VENDUTO'
      AND EXISTS (
          SELECT 1
          FROM Ticket_Detail td_focus
          WHERE td_focus.id_ticket = tg.id_ticket
            AND td_focus.scommessa IN ({scommesse_sql})
      )
    """

    query = f"""
        SELECT 
            tg.id_ticket,
            tg.ut_ins AS ticket_ut_ins,
            tg.data_ora_vend,
            tg.data_competenza,
            tg.cf,
            tg.nome_commerciale,
            td.palinsesto,
            td.manifestazione,
            td.avvenimento,
            td.scommessa,
            td.des_eve,
            td.descrizione_info_agg,
            td.quota,
            tg.importo_pagato
        FROM Ticket_General tg
        JOIN Ticket_Detail td 
            ON tg.id_ticket = td.id_ticket
        {where}
    """

    return pd.read_sql(query, engine)


def add_tuple_key(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()

    for col in TUPLE_FIELDS:
        if col not in df.columns:
            df[col] = ""

        df[col] = (
            df[col]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r"\|", " ", regex=True)
        )

        df[col] = df[col].replace("", "n/a")

    df["tuple"] = df[TUPLE_FIELDS].apply(lambda row: "|".join(row), axis=1)
    df["scommessa"] = pd.to_numeric(df["scommessa"], errors="coerce")
    df["quota"] = pd.to_numeric(df["quota"], errors="coerce")
    df["importo_pagato"] = pd.to_numeric(
        df["importo_pagato"],
        errors="coerce"
    ).fillna(0)

    return df


def filtra_solo_eventi_focus(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["scommessa"].isin(SCOMMESSE_FOCUS)].copy()


def importo_ticket_euro(importi) -> float:
    """
    Restituisce l'importo reale del ticket in euro.

    Nel dataset Ticket_General.importo_pagato è ripetuto su ogni riga evento
    dopo la JOIN con Ticket_Detail. Per questo NON va sommato.
    Si prende una sola occorrenza valida, usando il massimo per robustezza,
    e si divide per 100 perché il valore DB è espresso in centesimi.
    """
    valori = pd.to_numeric(pd.Series(importi), errors="coerce").dropna()

    if valori.empty:
        return 0.0

    return round(float(valori.max()) / 100, 2)


def aggregate_by_id(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["concessionario", "id_ticket"])
        .agg({
            "cf": "first",
            "nome_commerciale": "first",
            "tuple": list,
            "importo_pagato": list,
            "ticket_ut_ins": "min",
            "data_ora_vend": "min",
            "data_competenza": "min"
        })
        .reset_index()
    )


def aggregate_full_by_id(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["concessionario", "id_ticket"])
        .agg({
            "cf": "first",
            "nome_commerciale": "first",
            "tuple": list,
            "quota": list,
            "scommessa": list,
            "importo_pagato": list,
            "ticket_ut_ins": "min",
            "data_ora_vend": "min",
            "data_competenza": "min"
        })
        .reset_index()
    )


def genera_ripetute_identiche(tickets_agg: pd.DataFrame) -> pd.DataFrame:
    tickets_agg = tickets_agg.copy()

    tickets_agg["sequence"] = tickets_agg["tuple"].apply(
        lambda values: "|".join(sorted(values))
    )

    seq_counts = tickets_agg["sequence"].value_counts().to_dict()
    results = []

    for _, row in tickets_agg.iterrows():
        importo_ticket = importo_ticket_euro(row["importo_pagato"])
        count = seq_counts.get(row["sequence"], 0)

        if count >= OCCURRENCE_THRESHOLD and importo_ticket > SUM_THRESHOLD:
            results.append({
                "concessionario": row["concessionario"],
                "cf": row["cf"],
                "id_ticket": row["id_ticket"],
                "data_ora_vend": row["data_ora_vend"],
                "data_competenza": row["data_competenza"],
                "sequence": row["sequence"],
                "occurrence_count": count,
                "importo_totale_ticket": importo_ticket,
            })

    return pd.DataFrame(results)


def genera_ripetute_occulte(
    tickets_agg: pd.DataFrame,
    threshold: int
) -> pd.DataFrame:
    results = []
    records = tickets_agg.to_dict("records")

    for ticket_1, ticket_2 in combinations(records, 2):
        eventi_comuni = set(ticket_1["tuple"]).intersection(
            set(ticket_2["tuple"])
        )

        if len(eventi_comuni) >= threshold:
            results.append({
                "soglia": threshold,
                "concessionario_1": ticket_1["concessionario"],
                "nome_commerciale_1": ticket_1["nome_commerciale"],
                "cf_1": ticket_1["cf"],
                "id_ticket_1": ticket_1["id_ticket"],
                "data_ora_vend_1": ticket_1["data_ora_vend"],
                "data_competenza_1": ticket_1["data_competenza"],
                "concessionario_2": ticket_2["concessionario"],
                "nome_commerciale_2": ticket_2["nome_commerciale"],
                "cf_2": ticket_2["cf"],
                "id_ticket_2": ticket_2["id_ticket"],
                "data_ora_vend_2": ticket_2["data_ora_vend"],
                "data_competenza_2": ticket_2["data_competenza"],
                "eventi_in_comune": len(eventi_comuni),
                "tuple_comuni": " || ".join(sorted(eventi_comuni)),
                "importo_ticket_1": importo_ticket_euro(ticket_1["importo_pagato"]),
                "importo_ticket_2": importo_ticket_euro(ticket_2["importo_pagato"]),
            })

    return pd.DataFrame(results)


def conta_occulte_per_cf(df_occulte: pd.DataFrame, soglia: int) -> pd.DataFrame:
    col_name = f"num_occulte_{soglia}"

    if df_occulte.empty:
        return pd.DataFrame(columns=["concessionario", "cf", col_name])

    cf_1 = df_occulte[["concessionario_1", "cf_1"]].rename(
        columns={
            "concessionario_1": "concessionario",
            "cf_1": "cf"
        }
    )

    cf_2 = df_occulte[["concessionario_2", "cf_2"]].rename(
        columns={
            "concessionario_2": "concessionario",
            "cf_2": "cf"
        }
    )

    combined = pd.concat([cf_1, cf_2], ignore_index=True)

    return (
        combined
        .groupby(["concessionario", "cf"])
        .size()
        .reset_index(name=col_name)
    )


def crea_indice_occulte(lista_df_occulte: list[pd.DataFrame]) -> pd.DataFrame:
    lista_df_occulte = [
        df for df in lista_df_occulte
        if df is not None and not df.empty
    ]

    if not lista_df_occulte:
        return pd.DataFrame()

    df_occulte = reduce(
        lambda left, right: pd.merge(
            left,
            right,
            on=["concessionario", "cf"],
            how="outer"
        ),
        lista_df_occulte
    ).fillna(0)

    for soglia in OCCULT_THRESHOLDS:
        col = f"num_occulte_{soglia}"

        if col not in df_occulte.columns:
            df_occulte[col] = 0

        df_occulte[col] = df_occulte[col].astype(int)

    df_occulte["indice_occulte"] = (
        df_occulte["num_occulte_4"] * 1 +
        df_occulte["num_occulte_5"] * 2 +
        df_occulte["num_occulte_6"] * 3
    )

    df_occulte["indice_occulte_100"] = (
        df_occulte["indice_occulte"] * 10
    ).clip(upper=100)

    def assegna_classe(valore):
        if valore == 0:
            return "Nessuna"
        if valore <= 30:
            return "Bassa"
        if valore <= 60:
            return "Media"
        if valore <= 80:
            return "Alta"
        return "Molto Alta"

    df_occulte["classe_occulte"] = df_occulte["indice_occulte_100"].apply(
        assegna_classe
    )

    return df_occulte



def conta_occulte_per_nome_commerciale_cf(
    df_occulte: pd.DataFrame,
    soglia: int
) -> pd.DataFrame:
    """
    Conta le ripetute occulte mantenendo distinti:
    concessionario, nome_commerciale e codice fiscale.
    """
    col_name = f"num_occulte_{soglia}"

    if df_occulte.empty:
        return pd.DataFrame(
            columns=["concessionario", "nome_commerciale", "cf", col_name]
        )

    punto_cf_1 = df_occulte[
        ["concessionario_1", "nome_commerciale_1", "cf_1"]
    ].rename(columns={
        "concessionario_1": "concessionario",
        "nome_commerciale_1": "nome_commerciale",
        "cf_1": "cf"
    })

    punto_cf_2 = df_occulte[
        ["concessionario_2", "nome_commerciale_2", "cf_2"]
    ].rename(columns={
        "concessionario_2": "concessionario",
        "nome_commerciale_2": "nome_commerciale",
        "cf_2": "cf"
    })

    combined = pd.concat([punto_cf_1, punto_cf_2], ignore_index=True)

    return (
        combined
        .groupby(["concessionario", "nome_commerciale", "cf"], dropna=False)
        .size()
        .reset_index(name=col_name)
    )


def crea_indice_occulte_nome_commerciale(
    lista_df_occulte: list[pd.DataFrame]
) -> pd.DataFrame:
    """
    Produce l'indice per punto vendita e codice fiscale:
    concessionario | nome_commerciale | cf | contatori | indice | classe.
    """
    lista_df_occulte = [
        df for df in lista_df_occulte
        if df is not None and not df.empty
    ]

    if not lista_df_occulte:
        return pd.DataFrame()

    df_occulte = reduce(
        lambda left, right: pd.merge(
            left,
            right,
            on=["concessionario", "nome_commerciale", "cf"],
            how="outer"
        ),
        lista_df_occulte
    ).fillna(0)

    for soglia in OCCULT_THRESHOLDS:
        col = f"num_occulte_{soglia}"

        if col not in df_occulte.columns:
            df_occulte[col] = 0

        df_occulte[col] = df_occulte[col].astype(int)

    df_occulte["indice_occulte"] = (
        df_occulte["num_occulte_4"] * 1 +
        df_occulte["num_occulte_5"] * 2 +
        df_occulte["num_occulte_6"] * 3
    )

    df_occulte["indice_occulte_100"] = (
        df_occulte["indice_occulte"] * 10
    ).clip(upper=100)

    def assegna_classe(valore):
        if valore == 0:
            return "Nessuna"
        if valore <= 30:
            return "Bassa"
        if valore <= 60:
            return "Media"
        if valore <= 80:
            return "Alta"
        return "Molto Alta"

    df_occulte["classe_occulte"] = df_occulte["indice_occulte_100"].apply(
        assegna_classe
    )

    return df_occulte.sort_values(
        by=["indice_occulte_100", "indice_occulte"],
        ascending=[False, False]
    )


def crea_mappa_eventi_full(full_agg: pd.DataFrame) -> dict:
    full_map = {}

    for _, row in full_agg.iterrows():
        key = (row["concessionario"], row["id_ticket"])
        eventi = []

        for tuple_evento, quota, scommessa in zip(
            row["tuple"],
            row["quota"],
            row["scommessa"]
        ):
            eventi.append({
                "tuple": tuple_evento,
                "quota": quota,
                "scommessa": scommessa
            })

        full_map[key] = eventi

    return full_map


def genera_ticket_similari(
    tickets_focus_agg: pd.DataFrame,
    tickets_full_agg: pd.DataFrame
) -> pd.DataFrame:
    results = []
    focus_records = tickets_focus_agg.to_dict("records")
    full_map = crea_mappa_eventi_full(tickets_full_agg)

    for ticket_1, ticket_2 in combinations(focus_records, 2):
        key_1 = (ticket_1["concessionario"], ticket_1["id_ticket"])
        key_2 = (ticket_2["concessionario"], ticket_2["id_ticket"])

        focus_set_1 = set(ticket_1["tuple"])
        focus_set_2 = set(ticket_2["tuple"])

        eventi_comuni_focus = focus_set_1.intersection(focus_set_2)

        if len(eventi_comuni_focus) < 3:
            continue

        full_eventi_1 = full_map.get(key_1, [])
        full_eventi_2 = full_map.get(key_2, [])

        full_set_1 = set(e["tuple"] for e in full_eventi_1)
        full_set_2 = set(e["tuple"] for e in full_eventi_2)

        extra_1 = full_set_1 - full_set_2
        extra_2 = full_set_2 - full_set_1

        if len(extra_1) + len(extra_2) != 1:
            continue

        if len(extra_1) == 1:
            extra_tuple = list(extra_1)[0]
            extra_lato = "ticket_1"
            eventi_extra = full_eventi_1
        else:
            extra_tuple = list(extra_2)[0]
            extra_lato = "ticket_2"
            eventi_extra = full_eventi_2

        extra_quota = None
        extra_scommessa = None

        for evento in eventi_extra:
            if evento["tuple"] == extra_tuple:
                extra_quota = evento["quota"]
                extra_scommessa = evento["scommessa"]
                break

        if pd.isna(extra_quota):
            continue

        if extra_quota < QUOTA_EXTRA_THRESHOLD:
            results.append({
                "concessionario_1": ticket_1["concessionario"],
                "cf_1": ticket_1["cf"],
                "id_ticket_1": ticket_1["id_ticket"],
                "data_ora_vend_1": ticket_1["data_ora_vend"],
                "data_competenza_1": ticket_1["data_competenza"],
                "concessionario_2": ticket_2["concessionario"],
                "cf_2": ticket_2["cf"],
                "id_ticket_2": ticket_2["id_ticket"],
                "data_ora_vend_2": ticket_2["data_ora_vend"],
                "data_competenza_2": ticket_2["data_competenza"],
                "eventi_focus_in_comune": len(eventi_comuni_focus),
                "tuple_focus_comuni": " || ".join(sorted(eventi_comuni_focus)),
                "extra_su": extra_lato,
                "extra_tuple": extra_tuple,
                "extra_scommessa": extra_scommessa,
                "extra_quota": extra_quota,
                "quota_soglia": QUOTA_EXTRA_THRESHOLD,
                "importo_ticket_1": importo_ticket_euro(ticket_1["importo_pagato"]),
                "importo_ticket_2": importo_ticket_euro(ticket_2["importo_pagato"]),
            })

    return pd.DataFrame(results)


def genera_indice_quota_extra(
    df_similari: pd.DataFrame,
    tickets_full_agg: pd.DataFrame
) -> pd.DataFrame:
    tot_ticket = (
        tickets_full_agg
        .groupby(["concessionario", "cf"])
        .agg(tot_ticket_utente=("id_ticket", "nunique"))
        .reset_index()
    )

    if df_similari.empty:
        tot_ticket["num_ticket_quota_extra"] = 0
        tot_ticket["perc_ticket_quota_extra"] = 0.0
        return tot_ticket

    lato_1 = df_similari[[
        "concessionario_1",
        "cf_1",
        "id_ticket_1"
    ]].rename(columns={
        "concessionario_1": "concessionario",
        "cf_1": "cf",
        "id_ticket_1": "id_ticket"
    })

    lato_2 = df_similari[[
        "concessionario_2",
        "cf_2",
        "id_ticket_2"
    ]].rename(columns={
        "concessionario_2": "concessionario",
        "cf_2": "cf",
        "id_ticket_2": "id_ticket"
    })

    coinvolti = (
        pd.concat([lato_1, lato_2], ignore_index=True)
        .drop_duplicates()
    )

    quota_extra = (
        coinvolti
        .groupby(["concessionario", "cf"])
        .agg(num_ticket_quota_extra=("id_ticket", "nunique"))
        .reset_index()
    )

    result = tot_ticket.merge(
        quota_extra,
        on=["concessionario", "cf"],
        how="left"
    )

    result["num_ticket_quota_extra"] = (
        result["num_ticket_quota_extra"]
        .fillna(0)
        .astype(int)
    )

    result["perc_ticket_quota_extra"] = (
        result["num_ticket_quota_extra"] /
        result["tot_ticket_utente"] * 100
    ).round(2)

    return result


def genera_similarity_clustering_utente(
    tickets_full_agg: pd.DataFrame,
    similarity_minima: float = 50.0
) -> pd.DataFrame:
    results = []

    for (concessionario, cf), group in tickets_full_agg.groupby(
        ["concessionario", "cf"]
    ):
        records = group.to_dict("records")

        if len(records) < 2:
            continue

        cluster_utente = f"{concessionario}_{cf}"

        for ticket_1, ticket_2 in combinations(records, 2):
            set_1 = set(ticket_1["tuple"])
            set_2 = set(ticket_2["tuple"])

            eventi_comuni = set_1.intersection(set_2)
            eventi_totali = set_1.union(set_2)

            if not eventi_totali:
                continue

            similarity_percent = round(
                len(eventi_comuni) / len(eventi_totali) * 100,
                2
            )

            if similarity_percent < similarity_minima:
                continue

            quote_comuni = []

            for tuple_evento, quota in zip(ticket_1["tuple"], ticket_1["quota"]):
                if tuple_evento in eventi_comuni and pd.notna(quota):
                    try:
                        quote_comuni.append(float(quota))
                    except Exception:
                        pass

            peso_quota = (
                round(sum(quote_comuni) / len(quote_comuni), 2)
                if quote_comuni
                else 0
            )

            results.append({
                "cluster_utente": cluster_utente,
                "concessionario": concessionario,
                "cf": cf,
                "id_ticket_1": ticket_1["id_ticket"],
                "data_ora_vend_1": ticket_1["data_ora_vend"],
                "data_competenza_1": ticket_1["data_competenza"],
                "importo_ticket_1": importo_ticket_euro(ticket_1["importo_pagato"]),
                "id_ticket_2": ticket_2["id_ticket"],
                "data_ora_vend_2": ticket_2["data_ora_vend"],
                "data_competenza_2": ticket_2["data_competenza"],
                "importo_ticket_2": importo_ticket_euro(ticket_2["importo_pagato"]),
                "eventi_ticket_1": len(set_1),
                "eventi_ticket_2": len(set_2),
                "eventi_comuni": len(eventi_comuni),
                "similarity_percent": similarity_percent,
                "peso_quota": peso_quota,
                "tuple_comuni": " || ".join(sorted(eventi_comuni))
            })

    return pd.DataFrame(results)


def genera_indice_similarity_utenti(
    df_similarity: pd.DataFrame,
    df_indice_occulte: pd.DataFrame,
    df_indice_quota_extra: pd.DataFrame
) -> pd.DataFrame:
    if df_similarity.empty:
        base = pd.DataFrame(columns=[
            "concessionario",
            "cf",
            "indice_similarity",
            "peso_quota_medio",
            "num_confronti",
            "media_eventi_comuni"
        ])
    else:
        base = (
            df_similarity
            .groupby(["concessionario", "cf"])
            .agg({
                "similarity_percent": "mean",
                "peso_quota": "mean",
                "id_ticket_1": "count",
                "eventi_comuni": "mean"
            })
            .reset_index()
        )

        base = base.rename(columns={
            "similarity_percent": "indice_similarity",
            "peso_quota": "peso_quota_medio",
            "id_ticket_1": "num_confronti",
            "eventi_comuni": "media_eventi_comuni"
        })

        base["indice_similarity"] = base["indice_similarity"].round(2)
        base["peso_quota_medio"] = base["peso_quota_medio"].round(2)
        base["media_eventi_comuni"] = base["media_eventi_comuni"].round(2)

    result = base.copy()

    if df_indice_occulte is not None and not df_indice_occulte.empty:
        result = result.merge(
            df_indice_occulte,
            on=["concessionario", "cf"],
            how="outer"
        )

    if df_indice_quota_extra is not None and not df_indice_quota_extra.empty:
        result = result.merge(
            df_indice_quota_extra,
            on=["concessionario", "cf"],
            how="outer"
        )

    for col in ["num_occulte_4", "num_occulte_5", "num_occulte_6"]:
        if col not in result.columns:
            result[col] = 0
        result[col] = result[col].fillna(0).astype(int)

    if "indice_occulte" not in result.columns:
        result["indice_occulte"] = 0
    result["indice_occulte"] = result["indice_occulte"].fillna(0).astype(int)

    if "indice_occulte_100" not in result.columns:
        result["indice_occulte_100"] = 0
    result["indice_occulte_100"] = result["indice_occulte_100"].fillna(0).round(2)

    if "classe_occulte" not in result.columns:
        result["classe_occulte"] = "Nessuna"
    result["classe_occulte"] = result["classe_occulte"].fillna("Nessuna")

    for col in [
        "num_ticket_quota_extra",
        "tot_ticket_utente",
        "perc_ticket_quota_extra"
    ]:
        if col not in result.columns:
            result[col] = 0
        result[col] = result[col].fillna(0)

    result["num_ticket_quota_extra"] = result["num_ticket_quota_extra"].astype(int)
    result["tot_ticket_utente"] = result["tot_ticket_utente"].astype(int)
    result["perc_ticket_quota_extra"] = result["perc_ticket_quota_extra"].round(2)

    for col in [
        "indice_similarity",
        "peso_quota_medio",
        "num_confronti",
        "media_eventi_comuni"
    ]:
        if col not in result.columns:
            result[col] = 0
        result[col] = result[col].fillna(0)

    result["num_confronti"] = result["num_confronti"].astype(int)

    return result.sort_values(
        by=[
            "num_ticket_quota_extra",
            "perc_ticket_quota_extra",
            "indice_occulte_100",
            "indice_similarity",
            "num_confronti"
        ],
        ascending=[False, False, False, False, False]
    )


def invia_email(files: list[str], body: str):
    msg = EmailMessage()
    msg["Subject"] = "Report ripetute focus"
    msg["From"] = EMAIL_SENDER
    msg["To"] = ",".join(EMAIL_RECEIVERS)
    msg.set_content(body)

    for file_path in files:
        try:
            with open(file_path, "rb") as f:
                msg.add_attachment(
                    f.read(),
                    maintype="text",
                    subtype="csv",
                    filename=os.path.basename(file_path)
                )
        except Exception as e:
            logging.error(f"Errore allegando file {file_path}: {e}")

    try:
        with smtplib.SMTP(
            EMAIL_SMTP_SERVER,
            EMAIL_SMTP_PORT,
            timeout=30
        ) as smtp:
            smtp.starttls()
            smtp.login(EMAIL_SENDER, EMAIL_PASSWORD)
            smtp.send_message(msg)

        logging.info("Email inviata correttamente.")

    except Exception as e:
        logging.error(f"Errore invio email: {e}")


def main():
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    engines = crea_engines()
    all_frames = []

    for concessionario, engine in engines.items():
        try:
            logging.info(f"Caricamento ticket per {concessionario}...")

            df = carica_tickets(engine)
            df["concessionario"] = concessionario
            all_frames.append(df)

            logging.info(f"{concessionario}: caricati {len(df)} record.")

        except (ProgrammingError, OperationalError) as e:
            logging.warning(f"Database saltato {concessionario}: {e}")

        except Exception as e:
            logging.error(f"Errore su {concessionario}: {e}")

    if not all_frames:
        logging.info("Nessun database disponibile.")
        salva_csv(
            pd.DataFrame([{"stato": "nessun_database_disponibile"}]),
            "execution_status.csv"
        )
        return

    tk = pd.concat(all_frames, ignore_index=True)

    if tk.empty:
        logging.info("Nessun ticket trovato nella finestra temporale.")
        salva_csv(
            pd.DataFrame([{"stato": "nessun_ticket_trovato"}]),
            "execution_status.csv"
        )
        return

    logging.info(f"Totale record caricati: {len(tk)}")

    tk = add_tuple_key(tk)
    tk_focus = filtra_solo_eventi_focus(tk)

    if tk_focus.empty:
        logging.info("Nessun evento focus trovato nei ticket estratti.")
        salva_csv(
            pd.DataFrame([{"stato": "nessun_evento_focus_trovato"}]),
            "execution_status.csv"
        )
        return

    tickets_focus_agg = aggregate_by_id(tk_focus)
    tickets_full_agg = aggregate_full_by_id(tk)

    generated_files = []
    body_parts = []
    lista_occulte_per_cf = []
    lista_occulte_per_nome_commerciale_cf = []

    df_identiche = genera_ripetute_identiche(tickets_focus_agg)

    if not df_identiche.empty:
        file_path = salva_csv(df_identiche, "ripetute_tickets.csv")
        generated_files.append(file_path)
        body_parts.append(f"Ripetute identiche focus trovate: {len(df_identiche)}")

    for threshold in OCCULT_THRESHOLDS:
        df_occulte = genera_ripetute_occulte(tickets_focus_agg, threshold)

        df_occulte_cf = conta_occulte_per_cf(df_occulte, threshold)
        lista_occulte_per_cf.append(df_occulte_cf)

        df_occulte_nome_commerciale_cf = conta_occulte_per_nome_commerciale_cf(
            df_occulte,
            threshold
        )
        lista_occulte_per_nome_commerciale_cf.append(
            df_occulte_nome_commerciale_cf
        )

        if not df_occulte.empty:
            file_name = f"ripetute_occulte_{threshold}.csv"
            file_path = salva_csv(df_occulte, file_name)
            generated_files.append(file_path)
            body_parts.append(
                f"Ripetute occulte focus >= {threshold}: {len(df_occulte)}"
            )

    df_indice_occulte = crea_indice_occulte(lista_occulte_per_cf)

    if not df_indice_occulte.empty:
        file_path = salva_csv(df_indice_occulte, "indice_occulte_utenti.csv")
        generated_files.append(file_path)
        body_parts.append(
            f"Indice occulte utenti calcolato: {len(df_indice_occulte)} utenti"
        )

    df_indice_occulte_nome_commerciale = crea_indice_occulte_nome_commerciale(
        lista_occulte_per_nome_commerciale_cf
    )

    if not df_indice_occulte_nome_commerciale.empty:
        file_path = salva_csv(
            df_indice_occulte_nome_commerciale,
            "indice_occulte_nome_commerciale.csv"
        )
        generated_files.append(file_path)
        body_parts.append(
            "Indice occulte nome commerciale/CF calcolato: "
            f"{len(df_indice_occulte_nome_commerciale)} combinazioni"
        )

    df_similari = genera_ticket_similari(
        tickets_focus_agg,
        tickets_full_agg
    )

    if not df_similari.empty:
        file_path = salva_csv(df_similari, "ticket_similari.csv")
        generated_files.append(file_path)
        body_parts.append(f"Ticket similari trovati: {len(df_similari)}")

    df_indice_quota_extra = genera_indice_quota_extra(
        df_similari,
        tickets_full_agg
    )

    if not df_indice_quota_extra.empty:
        file_path = salva_csv(
            df_indice_quota_extra,
            "indice_quota_extra_utenti.csv"
        )
        generated_files.append(file_path)
        body_parts.append(
            f"Indice quota extra utenti calcolato: {len(df_indice_quota_extra)} utenti"
        )

    df_similarity_utenti = genera_similarity_clustering_utente(
        tickets_full_agg,
        similarity_minima=SIMILARITY_MINIMA_UTENTE
    )

    if not df_similarity_utenti.empty:
        file_path = salva_csv(
            df_similarity_utenti,
            "analisi_similarity_utenti.csv"
        )
        generated_files.append(file_path)
        body_parts.append(
            f"Cluster utenti con ticket simili trovati: {len(df_similarity_utenti)}"
        )

    df_indice_similarity = genera_indice_similarity_utenti(
        df_similarity_utenti,
        df_indice_occulte,
        df_indice_quota_extra
    )

    if not df_indice_similarity.empty:
        file_path = salva_csv(
            df_indice_similarity,
            "indice_similarity_utenti.csv"
        )
        generated_files.append(file_path)
        body_parts.append(
            f"Indice similarity utenti completo calcolato: {len(df_indice_similarity)} utenti"
        )

    if generated_files:
        body = "\n".join(body_parts)
        invia_email(generated_files, body)

        salva_csv(
            pd.DataFrame([{
                "stato": "report_generati",
                "numero_file_generati": len(generated_files),
                "file_generati": ", ".join(generated_files)
            }]),
            "execution_status.csv"
        )

        logging.info("Report generati.")
    else:
        salva_csv(
            pd.DataFrame([{"stato": "nessun_report_generato"}]),
            "execution_status.csv"
        )

        logging.info("Nessun report da inviare.")


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)

    except Exception:
        logging.exception("Errore durante l'esecuzione")

        try:
            os.makedirs(OUTPUT_DIR, exist_ok=True)
            pd.DataFrame([{"stato": "errore_esecuzione"}]).to_csv(
                os.path.join(OUTPUT_DIR, "execution_status.csv"),
                index=False,
                sep=CSV_SEPARATOR,
                decimal=CSV_DECIMAL,
                encoding="utf-8-sig"
            )
        except Exception:
            pass

        sys.exit(1)
