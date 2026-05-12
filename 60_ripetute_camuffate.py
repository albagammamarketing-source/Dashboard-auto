import sys
import pandas as pd
import logging
import smtplib
from itertools import combinations
from email.message import EmailMessage
from sqlalchemy import create_engine
from sqlalchemy.exc import ProgrammingError, OperationalError

# --- Configurazioni generali ---
FETCH_WINDOW_MINUTES = 10000

DATA_INIZIO = None
DATA_FINE = None

# Se vuoi usare FETCH_WINDOW_MINUTES:
# DATA_INIZIO = None o "2026-04-30 23:59:59"
# DATA_FINE = None

SUM_THRESHOLD = 1
OCCURRENCE_THRESHOLD = 2
OCCULT_THRESHOLDS = [4, 5, 6]
QUOTA_EXTRA_THRESHOLD = 108

SIMILARITY_MINIMA_UTENTE = 50.0
CSV_SEPARATOR = ";"
CSV_DECIMAL = ","

pd.set_option("future.no_silent_downcasting", True)

SCOMMESSE_FOCUS = [
    1, 13353, 16035, 16177, 16190, 16474, 17589, 17637, 19804,
    21147, 22580, 24481, 26435, 26437, 26441, 26443, 26445,
    26446, 26450, 26454, 26465, 26466, 26468
]

EMAIL_SENDER = "albagamma.marketing@gmail.com"
EMAIL_PASSWORD = "jjel rwtf jmxb cute"
EMAIL_SMTP_SERVER = "smtp.gmail.com"
EMAIL_SMTP_PORT = 587
EMAIL_RECEIVERS = ["dario.guarriello@gmail.com"]

DB_CONFIGS = {
    "360BET": {"user": "dbalba11", "password": "Albagamma2024$", "host": "194.163.157.255", "port": 3306, "database": "AnalisiTickets_360BET"},
    "ADMIRAL": {"user": "dbalba11", "password": "Albagamma2024$", "host": "194.163.157.255", "port": 3306, "database": "AnalisiTickets_ADMIRAL"},
    "BBET": {"user": "dbalba11", "password": "Albagamma2024$", "host": "194.163.157.255", "port": 3306, "database": "AnalisiTickets_BBET"},
    "DOMUSBET": {"user": "dbalba11", "password": "Albagamma2024$", "host": "194.163.157.255", "port": 3306, "database": "AnalisiTickets_DOMUSBET"},
    "MARATHON": {"user": "dbalba11", "password": "Albagamma2024$", "host": "194.163.157.255", "port": 3306, "database": "AnalisiTickets_MARATHON"},
    "SKYWIND": {"user": "dbalba11", "password": "Albagamma2024$", "host": "194.163.157.255", "port": 3306, "database": "AnalisiTickets_SKYWIND"},
    "SPORTBET": {"user": "dbalba11", "password": "Albagamma2024$", "host": "194.163.157.255", "port": 3306, "database": "AnalisiTickets_SPORTBET"},
    "STANLEYBET": {"user": "dbalba11", "password": "Albagamma2024$", "host": "194.163.157.255", "port": 3306, "database": "AnalisiTickets_STANLEYBET"},
    "STARCASINO": {"user": "dbalba11", "password": "Albagamma2024$", "host": "194.163.157.255", "port": 3306, "database": "AnalisiTickets_STARCASINO"},
    "TOTOSI": {"user": "dbalba11", "password": "Albagamma2024$", "host": "194.163.157.255", "port": 3306, "database": "AnalisiTickets_TOTOSI"},
    "VINCITU": {"user": "dbalba11", "password": "Albagamma2024$", "host": "194.163.157.255", "port": 3306, "database": "AnalisiTickets_VINCITU"},
    "WILLIAMHILL": {"user": "dbalba11", "password": "Albagamma2024$", "host": "194.163.157.255", "port": 3306, "database": "AnalisiTickets_WILLIAMHILL"},
    "IGT": {"user": "dbalba11", "password": "Albagamma2024$", "host": "194.163.157.255", "port": 3306, "database": "AnalisiTickets_IGT"},
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


def salva_csv(df: pd.DataFrame, file_path: str):
    df.to_csv(
        file_path,
        index=False,
        sep=CSV_SEPARATOR,
        decimal=CSV_DECIMAL,
        encoding="utf-8-sig"
    )


def crea_engines():
    return {
        name: create_engine(
            f"mysql+mysqlconnector://{cfg['user']}:{cfg['password']}@"
            f"{cfg['host']}:{cfg['port']}/{cfg['database']}"
        )
        for name, cfg in DB_CONFIGS.items()
    }


def build_filter(alias: str) -> str:
    usa_intervallo_date = (
        DATA_INIZIO is not None
        and DATA_FINE is not None
        and str(DATA_INIZIO).strip() != ""
        and str(DATA_FINE).strip() != ""
    )

    if usa_intervallo_date:
        logging.info(
            f"Filtro attivo su {alias}.data_ora_vend da {DATA_INIZIO} a {DATA_FINE}"
        )
        return BASE_FILTER_DATE.format(
            alias=alias,
            data_inizio=DATA_INIZIO,
            data_fine=DATA_FINE
        )

    logging.info(
        f"Filtro attivo su {alias}.ut_ins: ultimi {FETCH_WINDOW_MINUTES} minuti"
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
        df[col] = (
            df[col]
            .fillna("")
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r"\|", " ", regex=True)
        )
        df[col] = df[col].replace("", "n/a")

    df["tuple"] = df[TUPLE_FIELDS].apply(
        lambda row: "|".join(row),
        axis=1
    )

    df["scommessa"] = pd.to_numeric(df["scommessa"], errors="coerce")
    df["quota"] = pd.to_numeric(df["quota"], errors="coerce")

    return df


def filtra_solo_eventi_focus(df: pd.DataFrame) -> pd.DataFrame:
    return df[df["scommessa"].isin(SCOMMESSE_FOCUS)].copy()


def aggregate_by_id(df: pd.DataFrame) -> pd.DataFrame:
    return (
        df.groupby(["concessionario", "id_ticket"])
        .agg({
            "cf": "first",
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
        total = sum(row["importo_pagato"])
        count = seq_counts.get(row["sequence"], 0)

        if count >= OCCURRENCE_THRESHOLD and total > SUM_THRESHOLD:
            results.append({
                "concessionario": row["concessionario"],
                "cf": row["cf"],
                "id_ticket": row["id_ticket"],
                "data_ora_vend": row["data_ora_vend"],
                "data_competenza": row["data_competenza"],
                "sequence": row["sequence"],
                "occurrence_count": count,
                "importo_totale_ticket": total,
            })

    return pd.DataFrame(results)


def genera_ripetute_occulte(tickets_agg: pd.DataFrame, threshold: int) -> pd.DataFrame:
    results = []
    records = tickets_agg.to_dict("records")

    for ticket_1, ticket_2 in combinations(records, 2):
        eventi_comuni = set(ticket_1["tuple"]).intersection(set(ticket_2["tuple"]))

        if len(eventi_comuni) >= threshold:
            results.append({
                "soglia": threshold,
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
                "eventi_in_comune": len(eventi_comuni),
                "tuple_comuni": " || ".join(sorted(eventi_comuni)),
                "importo_ticket_1": sum(ticket_1["importo_pagato"]),
                "importo_ticket_2": sum(ticket_2["importo_pagato"]),
            })

    return pd.DataFrame(results)


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

        if len(eventi_comuni_focus) < 4:
            continue

        full_eventi_1 = full_map.get(key_1, [])
        full_eventi_2 = full_map.get(key_2, [])

        full_set_1 = set(e["tuple"] for e in full_eventi_1)
        full_set_2 = set(e["tuple"] for e in full_eventi_2)

        extra_1 = full_set_1 - full_set_2
        extra_2 = full_set_2 - full_set_1

        if len(extra_1) + len(extra_2) != 1:
            continue

        extra_tuple = list(extra_1)[0] if len(extra_1) == 1 else list(extra_2)[0]
        extra_lato = "ticket_1" if len(extra_1) == 1 else "ticket_2"
        eventi_extra = full_eventi_1 if extra_lato == "ticket_1" else full_eventi_2

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
                "importo_ticket_1": sum(ticket_1["importo_pagato"]),
                "importo_ticket_2": sum(ticket_2["importo_pagato"]),
            })

    return pd.DataFrame(results)


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
                "importo_ticket_1": sum(ticket_1["importo_pagato"]),
                "id_ticket_2": ticket_2["id_ticket"],
                "data_ora_vend_2": ticket_2["data_ora_vend"],
                "data_competenza_2": ticket_2["data_competenza"],
                "importo_ticket_2": sum(ticket_2["importo_pagato"]),
                "eventi_ticket_1": len(set_1),
                "eventi_ticket_2": len(set_2),
                "eventi_comuni": len(eventi_comuni),
                "similarity_percent": similarity_percent,
                "peso_quota": peso_quota,
                "tuple_comuni": " || ".join(sorted(eventi_comuni))
            })

    return pd.DataFrame(results)


def genera_indice_similarity_utenti(df_similarity: pd.DataFrame) -> pd.DataFrame:
    if df_similarity.empty:
        return pd.DataFrame()

    result = (
        df_similarity.groupby(["concessionario", "cf"])
        .agg({
            "similarity_percent": "mean",
            "peso_quota": "mean",
            "id_ticket_1": "count",
            "eventi_comuni": "mean"
        })
        .reset_index()
    )

    result = result.rename(columns={
        "similarity_percent": "indice_similarity",
        "peso_quota": "peso_quota_medio",
        "id_ticket_1": "num_confronti",
        "eventi_comuni": "media_eventi_comuni"
    })

    result["indice_similarity"] = result["indice_similarity"].round(2)
    result["peso_quota_medio"] = result["peso_quota_medio"].round(2)
    result["media_eventi_comuni"] = result["media_eventi_comuni"].round(2)

    return result.sort_values(
        by=["indice_similarity", "num_confronti", "peso_quota_medio"],
        ascending=[False, False, False]
    )


def invia_email(files: list[str], body: str):
    msg = EmailMessage()
    msg["Subject"] = "Report ripetute focus"
    msg["From"] = EMAIL_SENDER
    msg["To"] = ",".join(EMAIL_RECEIVERS)
    msg.set_content(body)

    for file_path in files:
        with open(file_path, "rb") as f:
            msg.add_attachment(
                f.read(),
                maintype="text",
                subtype="csv",
                filename=file_path
            )

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
            logging.warning(f"Skip {concessionario}: {e}")

        except Exception as e:
            logging.error(f"Errore su {concessionario}: {e}")

    if not all_frames:
        logging.info("Nessun database disponibile. Termino.")
        return

    tk = pd.concat(all_frames, ignore_index=True)

    if tk.empty:
        logging.info("Nessun ticket trovato nella finestra temporale.")
        return

    tk = add_tuple_key(tk)
    tk_focus = filtra_solo_eventi_focus(tk)

    if tk_focus.empty:
        logging.info("Nessun evento focus trovato nei ticket estratti.")
        return

    tickets_focus_agg = aggregate_by_id(tk_focus)
    tickets_full_agg = aggregate_full_by_id(tk)

    generated_files = []
    body_parts = []

    df_identiche = genera_ripetute_identiche(tickets_focus_agg)

    if not df_identiche.empty:
        file_identiche = "ripetute_tickets.csv"
        salva_csv(df_identiche, file_identiche)
        generated_files.append(file_identiche)
        body_parts.append(f"Ripetute identiche focus trovate: {len(df_identiche)}")
        logging.info(f"Generato: {file_identiche}")

    for threshold in OCCULT_THRESHOLDS:
        df_occulte = genera_ripetute_occulte(tickets_focus_agg, threshold)

        if not df_occulte.empty:
            file_occulte = f"ripetute_occulte_{threshold}.csv"
            salva_csv(df_occulte, file_occulte)
            generated_files.append(file_occulte)
            body_parts.append(
                f"Ripetute occulte focus >= {threshold}: {len(df_occulte)}"
            )
            logging.info(f"Generato: {file_occulte}")

    df_similari = genera_ticket_similari(
        tickets_focus_agg,
        tickets_full_agg
    )

    if not df_similari.empty:
        file_similari = "ticket_similari.csv"
        salva_csv(df_similari, file_similari)
        generated_files.append(file_similari)
        body_parts.append(f"Ticket similari trovati: {len(df_similari)}")
        logging.info(f"Generato: {file_similari}")

    df_similarity_utenti = genera_similarity_clustering_utente(
        tickets_full_agg,
        similarity_minima=SIMILARITY_MINIMA_UTENTE
    )

    if not df_similarity_utenti.empty:
        file_similarity = "analisi_similarity_utenti.csv"
        salva_csv(df_similarity_utenti, file_similarity)
        generated_files.append(file_similarity)
        body_parts.append(
            f"Cluster utenti con ticket simili trovati: {len(df_similarity_utenti)}"
        )
        logging.info(f"Generato: {file_similarity}")

        df_indice_similarity = genera_indice_similarity_utenti(
            df_similarity_utenti
        )

        if not df_indice_similarity.empty:
            file_indice = "indice_similarity_utenti.csv"
            salva_csv(df_indice_similarity, file_indice)
            generated_files.append(file_indice)
            body_parts.append(
                f"Indice similarity utenti calcolato: {len(df_indice_similarity)} utenti"
            )
            logging.info(f"Generato: {file_indice}")
    else:
        logging.info("Nessun cluster utente con similarity significativa trovato.")

    if generated_files:
        body = "\n".join(body_parts)
        invia_email(generated_files, body)
        logging.info("Report generati e inviati.")
    else:
        logging.info("Nessun report da inviare.")


if __name__ == "__main__":
    try:
        main()
        sys.exit(0)

    except Exception:
        logging.exception("Errore durante l'esecuzione")
        sys.exit(1)