from pathlib import Path

src = Path("/mnt/data/Pasted text.txt")
dst = Path("/mnt/data/60_ripetute_camuffate_GITHUB_nome_commerciale_CF.py")

text = src.read_text(encoding="utf-8")

# 1) Estraggo nome_commerciale dal database.
old_query = """            tg.cf,
            td.palinsesto,"""
new_query = """            tg.cf,
            tg.nome_commerciale,
            td.palinsesto,"""
if old_query not in text:
    raise RuntimeError("Blocco SELECT con tg.cf non individuato.")
text = text.replace(old_query, new_query, 1)

# 2) Trasporto nome_commerciale nelle aggregazioni ticket focus e complete.
old_agg = '''        .agg({
            "cf": "first",
            "tuple": list,'''
new_agg = '''        .agg({
            "cf": "first",
            "nome_commerciale": "first",
            "tuple": list,'''
if text.count(old_agg) != 2:
    raise RuntimeError("Non sono state individuate entrambe le aggregazioni da aggiornare.")
text = text.replace(old_agg, new_agg)

# 3) Aggiungo nome_commerciale ai record delle ripetute occulte.
old_occ_1 = '''                "concessionario_1": ticket_1["concessionario"],
                "cf_1": ticket_1["cf"],
                "id_ticket_1": ticket_1["id_ticket"],'''
new_occ_1 = '''                "concessionario_1": ticket_1["concessionario"],
                "nome_commerciale_1": ticket_1["nome_commerciale"],
                "cf_1": ticket_1["cf"],
                "id_ticket_1": ticket_1["id_ticket"],'''
old_occ_2 = '''                "concessionario_2": ticket_2["concessionario"],
                "cf_2": ticket_2["cf"],
                "id_ticket_2": ticket_2["id_ticket"],'''
new_occ_2 = '''                "concessionario_2": ticket_2["concessionario"],
                "nome_commerciale_2": ticket_2["nome_commerciale"],
                "cf_2": ticket_2["cf"],
                "id_ticket_2": ticket_2["id_ticket"],'''
if old_occ_1 not in text or old_occ_2 not in text:
    raise RuntimeError("Blocco genera_ripetute_occulte non individuato.")
text = text.replace(old_occ_1, new_occ_1, 1)
text = text.replace(old_occ_2, new_occ_2, 1)

# 4) Aggiungo le funzioni del nuovo indice, subito prima della mappa eventi full.
anchor = "\ndef crea_mappa_eventi_full(full_agg: pd.DataFrame) -> dict:\n"
if anchor not in text:
    raise RuntimeError("Punto di inserimento funzioni nuovo indice non individuato.")

new_functions = r'''

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

'''
text = text.replace(anchor, new_functions + anchor, 1)

# 5) Creo la lista in main per il nuovo indice.
old_lists = '''    body_parts = []
    lista_occulte_per_cf = []
'''
new_lists = '''    body_parts = []
    lista_occulte_per_cf = []
    lista_occulte_per_nome_commerciale_cf = []
'''
if old_lists not in text:
    raise RuntimeError("Inizializzazione liste in main non individuata.")
text = text.replace(old_lists, new_lists, 1)

# 6) Durante il ciclo sulle soglie, popolo anche il conteggio nome_commerciale + cf.
old_loop = '''        df_occulte_cf = conta_occulte_per_cf(df_occulte, threshold)
        lista_occulte_per_cf.append(df_occulte_cf)

        if not df_occulte.empty:'''
new_loop = '''        df_occulte_cf = conta_occulte_per_cf(df_occulte, threshold)
        lista_occulte_per_cf.append(df_occulte_cf)

        df_occulte_nome_commerciale_cf = conta_occulte_per_nome_commerciale_cf(
            df_occulte,
            threshold
        )
        lista_occulte_per_nome_commerciale_cf.append(
            df_occulte_nome_commerciale_cf
        )

        if not df_occulte.empty:'''
if old_loop not in text:
    raise RuntimeError("Ciclo occulte in main non individuato.")
text = text.replace(old_loop, new_loop, 1)

# 7) Genero e allego il nuovo CSV dopo l'indice occulte utenti.
old_after_user_index = '''    if not df_indice_occulte.empty:
        file_path = salva_csv(df_indice_occulte, "indice_occulte_utenti.csv")
        generated_files.append(file_path)
        body_parts.append(
            f"Indice occulte utenti calcolato: {len(df_indice_occulte)} utenti"
        )

    df_similari = genera_ticket_similari('''
new_after_user_index = '''    if not df_indice_occulte.empty:
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

    df_similari = genera_ticket_similari('''
if old_after_user_index not in text:
    raise RuntimeError("Blocco indice occulte utenti in main non individuato.")
text = text.replace(old_after_user_index, new_after_user_index, 1)

# Scrittura e verifica sintattica.
dst.write_text(text, encoding="utf-8")
compile(text, str(dst), "exec")

# Controlli di presenza delle integrazioni essenziali.
required_strings = [
    "tg.nome_commerciale",
    "nome_commerciale_1",
    "nome_commerciale_2",
    "def conta_occulte_per_nome_commerciale_cf",
    "def crea_indice_occulte_nome_commerciale",
    '"indice_occulte_nome_commerciale.csv"',
    '["concessionario", "nome_commerciale", "cf"]'
]
missing = [item for item in required_strings if item not in text]
if missing:
    raise RuntimeError("Controlli finali falliti: " + ", ".join(missing))

print(f"Creato: {dst.name}")
print("Sintassi verificata: OK")
print("Nuovo output: output/indice_occulte_nome_commerciale.csv")
print("Chiave di aggregazione: concessionario + nome_commerciale + cf")
