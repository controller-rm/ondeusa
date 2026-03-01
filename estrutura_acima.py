from __future__ import annotations

import os
from collections import defaultdict, deque
from datetime import date

import mysql.connector
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

import plotly.express as px


# =========================================================
# APP
# =========================================================
st.set_page_config(layout="wide", page_title="Estrutura Acima + Consolidado", page_icon="🧩")
st.title("🧩 Estrutura Acima — trilhas + 🧾 Consolidado (Estoque/Compra/Req/Faturamento) no Detalhamento")

st.caption(
    "1) Clique em **Buscar** para carregar a estrutura do produto.\n"
    "2) Depois, use **Aplicar filtros** (sem recarregar do zero)."
)

# =========================================================
# .ENV / CREDENCIAIS
# =========================================================
load_dotenv()
MYSQL_HOST = os.getenv("MYSQL_HOST")
MYSQL_PORT = int(os.getenv("MYSQL_PORT") or 3306)
MYSQL_DATABASE = os.getenv("MYSQL_DATABASE")
MYSQL_USER = os.getenv("MYSQL_USER")
MYSQL_PASSWORD = os.getenv("MYSQL_PASSWORD")


# =========================================================
# CONEXÃO / HELPERS
# =========================================================
def connect_to_mysql():
    try:
        return mysql.connector.connect(
            host=MYSQL_HOST,
            port=MYSQL_PORT,
            user=MYSQL_USER,
            password=MYSQL_PASSWORD,
            database=MYSQL_DATABASE,
        )
    except Exception as e:
        st.error(f"Erro ao conectar no MySQL: {e}")
        return None


def fetch_df(query: str, params=None) -> pd.DataFrame:
    conn = connect_to_mysql()
    if conn is None:
        return pd.DataFrame()
    try:
        cur = conn.cursor()
        cur.execute(query, params or [])
        rows = cur.fetchall()
        cols = [d[0] for d in cur.description] if cur.description else []
        return pd.DataFrame(rows, columns=cols)
    except Exception as e:
        st.error(f"Erro ao executar query: {e}")
        return pd.DataFrame()
    finally:
        try:
            conn.close()
        except Exception:
            pass


def _norm_str(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    return str(x).strip()


def build_in_clause(values):
    """Retorna (' IN (%s,%s,...)', params) ou ('',[])"""
    if not values:
        return "", []
    placeholders = ",".join(["%s"] * len(values))
    return f" IN ({placeholders})", list(values)


def _apply_inclusion_exclusion(
    df: pd.DataFrame,
    col: str,
    incluir: list[str] | None,
    excluir: list[str] | None,
):
    if df.empty or col not in df.columns:
        return df

    s = df[col].astype(str).map(_norm_str)
    df = df.copy()
    df[col] = s

    if incluir:
        incluir_set = set(map(_norm_str, incluir))
        df = df[df[col].isin(incluir_set)]

    if excluir:
        excluir_set = set(map(_norm_str, excluir))
        df = df[~df[col].isin(excluir_set)]

    return df


def _options_from(df, col):
    if df.empty or col not in df.columns:
        return []
    return sorted([x for x in df[col].astype(str).map(_norm_str).unique().tolist() if x != ""])


# =========================================================
# LOADERS (MYSQL) - ESTRUTURA / PRODUTO / ÚLTIMA VENDA
# =========================================================
@st.cache_data(show_spinner=False, ttl=300)
def carregar_estrutura() -> pd.DataFrame:
    conn = connect_to_mysql()
    if conn is None:
        return pd.DataFrame()

    sql = """
        SELECT
            codigo_empresa,
            codigo_filial,
            produto,
            sequencia,
            componente,
            quantidade,
            medida_1_corte,
            medida_2_corte,
            nro_partes,
            nro_item_desenho,
            obs_ligacao,
            qtde_referencial,
            tem_regra_existencia,
            perc_na_formula,
            codigo_desenho,
            data_inclusao,
            hora_inclusao,
            usuario_inclusao,
            data_alteracao,
            hora_alteracao,
            usuario_alteracao,
            qtde_prod_minuto,
            ult_preco_compra,
            ult_custo_medio
        FROM ESTRUTURA
    """
    try:
        df = pd.read_sql(sql, conn)
        return df
    except Exception as e:
        st.error(f"Erro ao ler ESTRUTURA: {e}")
        return pd.DataFrame()
    finally:
        try:
            conn.close()
        except Exception:
            pass


@st.cache_data(show_spinner=False, ttl=300)
def carregar_produto() -> pd.DataFrame:
    conn = connect_to_mysql()
    if conn is None:
        return pd.DataFrame()

    sql = """
        SELECT
            codigo_produto_material,
            tipo_material,
            GP_codigo_grupo,
            SGP_codigo_subgrupo,
            LP_codigo_linha
        FROM PRODUTO
    """
    try:
        df = pd.read_sql(sql, conn)
        return df
    except Exception as e:
        st.error(f"Erro ao ler PRODUTO: {e}")
        return pd.DataFrame()
    finally:
        try:
            conn.close()
        except Exception:
            pass


@st.cache_data(show_spinner=False, ttl=300)
def carregar_rve520_ultima_venda() -> pd.DataFrame:
    """
    Retorna: CODIGO_PRODUTO, ultima_venda (max DATA_MOVTO_T)
    """
    conn = connect_to_mysql()
    if conn is None:
        return pd.DataFrame()

    sql = """
        SELECT
            CODIGO_PRODUTO,
            DATA_MOVTO_T
        FROM RVE520CSV2
        WHERE CODIGO_PRODUTO IS NOT NULL
          AND DATA_MOVTO_T IS NOT NULL
    """
    try:
        df = pd.read_sql(sql, conn)
        if df.empty:
            return df

        df["CODIGO_PRODUTO"] = df["CODIGO_PRODUTO"].astype(str).map(_norm_str)
        df["DATA_MOVTO_T"] = pd.to_datetime(df["DATA_MOVTO_T"], errors="coerce", dayfirst=True)

        ult = (
            df.dropna(subset=["DATA_MOVTO_T"])
              .groupby("CODIGO_PRODUTO", as_index=False)["DATA_MOVTO_T"]
              .max()
              .rename(columns={"DATA_MOVTO_T": "ultima_venda"})
        )
        return ult
    except Exception as e:
        st.error(f"Erro ao ler RVE520CSV2: {e}")
        return pd.DataFrame()
    finally:
        try:
            conn.close()
        except Exception:
            pass


# =========================================================
# OPÇÕES DOS FILTROS (DISTINCT) - CONSOLIDADO (ESTOQUE/REQ)
# =========================================================
@st.cache_data(ttl=300)
def load_filter_options_consolidado():
    df_dep = fetch_df("""
        SELECT DISTINCT TRIM(deposito) AS deposito
        FROM POSICAO_ESTOQUE_ATUAL
        WHERE deposito IS NOT NULL AND TRIM(deposito) <> ''
        ORDER BY deposito
    """)

    df_opts = fetch_df("""
        SELECT
            TRIM(grupo) AS grupo,
            TRIM(subgrupo) AS subgrupo,
            TRIM(linha) AS linha,
            TRIM(tipo_material) AS tipo_material
        FROM POSICAO_ESTOQUE_ATUAL
    """)

    df_status = fetch_df("""
        SELECT DISTINCT TRIM(status) AS status
        FROM REQUISICOES
        WHERE status IS NOT NULL AND TRIM(status) <> ''
        ORDER BY status
    """)

    def clean_list(s: pd.Series):
        if s is None or s.empty:
            return []
        return (
            s.dropna()
             .astype(str)
             .map(lambda x: x.strip())
             .loc[lambda x: x != ""]
             .drop_duplicates()
             .sort_values()
             .tolist()
        )

    return {
        "deposito": clean_list(df_dep.get("deposito", pd.Series(dtype=str))),
        "grupo": clean_list(df_opts.get("grupo", pd.Series(dtype=str))),
        "subgrupo": clean_list(df_opts.get("subgrupo", pd.Series(dtype=str))),
        "linha": clean_list(df_opts.get("linha", pd.Series(dtype=str))),
        "tipo_material": clean_list(df_opts.get("tipo_material", pd.Series(dtype=str))),
        "status": clean_list(df_status.get("status", pd.Series(dtype=str))),
    }


# =========================================================
# CONSOLIDADO POR LISTA DE PRODUTOS (aplica mesmas regras do 2º app)
# =========================================================
@st.cache_data(ttl=300)
def load_consolidado_por_produtos(
    produtos: list[str],
    depositos: list[str],
    grupos: list[str],
    subgrupos: list[str],
    linhas: list[str],
    tipos: list[str],
    status_list: list[str],
    somente_com_saldo: bool,
    somente_sem_req: bool,
    somente_com_venda: bool,
) -> pd.DataFrame:
    produtos = [p for p in map(_norm_str, produtos) if p]
    if not produtos:
        return pd.DataFrame()

    produtos = produtos[:5000]

    where_parts = []
    params = []

    in_clause, in_params = build_in_clause(produtos)
    where_parts.append(f"TRIM(pe.produto){in_clause}")
    params += in_params

    in_clause, in_params = build_in_clause(depositos)
    if in_clause:
        where_parts.append(f"TRIM(pe.deposito){in_clause}")
        params += in_params

    in_clause, in_params = build_in_clause(grupos)
    if in_clause:
        where_parts.append(f"TRIM(pe.grupo){in_clause}")
        params += in_params

    in_clause, in_params = build_in_clause(subgrupos)
    if in_clause:
        where_parts.append(f"TRIM(pe.subgrupo){in_clause}")
        params += in_params

    in_clause, in_params = build_in_clause(linhas)
    if in_clause:
        where_parts.append(f"TRIM(pe.linha){in_clause}")
        params += in_params

    in_clause, in_params = build_in_clause(tipos)
    if in_clause:
        where_parts.append(f"TRIM(pe.tipo_material){in_clause}")
        params += in_params

    where_estoque = "WHERE " + " AND ".join(where_parts)

    status_sql = ""
    status_params = []
    in_clause, in_params = build_in_clause(status_list)
    if in_clause:
        status_sql = f" AND TRIM(r.status){in_clause} "
        status_params += in_params

    where_final_parts = []
    if somente_com_saldo:
        where_final_parts.append("e.quantidade_estoque > 0")
    if somente_sem_req:
        where_final_parts.append("(ra.produto IS NULL OR COALESCE(ra.quantidade_requisitada,0) = 0)")
    if somente_com_venda:
        where_final_parts.append("uf.data_ultimo_faturamento IS NOT NULL")
    where_final = ("WHERE " + " AND ".join(where_final_parts)) if where_final_parts else ""

    query = f"""
    WITH
    estoque AS (
        SELECT
            TRIM(pe.produto) AS produto,
            MAX(pe.descricao_material) AS descricao_material,
            MAX(pe.tipo_material) AS tipo_material,
            SUM(COALESCE(pe.quantidade,0)) AS quantidade_estoque,
            SUM(COALESCE(pe.quantidade,0) * COALESCE(pe.custo_unitario,0)) AS custo_total
        FROM POSICAO_ESTOQUE_ATUAL pe
        {where_estoque}
        GROUP BY TRIM(pe.produto)
    ),
    ult_compra AS (
        SELECT
            inf.codigo_produto AS produto,
            MAX(nf.data_recepcao_documento) AS data_ultima_compra
        FROM NOTA_FISCAL nf
        INNER JOIN ITENS_NOTA_FISCAL inf
            ON nf.codigo_cliente_fornecedor = inf.codigo_cliente_fornecedor
           AND nf.numero_documento          = inf.numero_documento
           AND nf.serie_documento           = inf.serie_documento
        WHERE nf.tipo_documento = 'E'
        GROUP BY inf.codigo_produto
    ),
    ult_compra_qtd AS (
        SELECT
            inf.codigo_produto AS produto,
            nf.data_recepcao_documento AS data_ultima_compra,
            SUM(inf.quantidade) AS quantidade_compra,
            CASE
                WHEN COALESCE(SUM(inf.quantidade),0) = 0 THEN NULL
                ELSE SUM(inf.valor_total) / SUM(inf.quantidade)
            END AS ult_preco_unit_compra
        FROM NOTA_FISCAL nf
        INNER JOIN ITENS_NOTA_FISCAL inf
            ON nf.codigo_cliente_fornecedor = inf.codigo_cliente_fornecedor
           AND nf.numero_documento          = inf.numero_documento
           AND nf.serie_documento           = inf.serie_documento
        INNER JOIN ult_compra uc
            ON uc.produto = inf.codigo_produto
           AND uc.data_ultima_compra = nf.data_recepcao_documento
        WHERE nf.tipo_documento = 'E'
        GROUP BY inf.codigo_produto, nf.data_recepcao_documento
    ),
    req_agg AS (
        SELECT
            r.material AS produto,
            MIN(r.data_abertura) AS data_primeira_requisicao,
            MAX(r.data_abertura) AS data_ultima_requisicao,
            SUM(r.quantidade)    AS quantidade_requisitada,
            MAX(r.produto_of)    AS produto_of,
            MAX(r.numero_da_of)  AS numero_da_of
        FROM REQUISICOES r
        LEFT JOIN ult_compra_qtd ucq
            ON ucq.produto = r.material
        WHERE DATE(r.data_abertura) BETWEEN
            COALESCE(DATE(ucq.data_ultima_compra), '1900-01-01')
            AND CURDATE()
        {status_sql}
        GROUP BY r.material
    ),
    ult_faturamento AS (
        SELECT
            SUBSTRING_INDEX(TRIM(CODIGO_PRODUTO), '.', 1) AS produto_venda,
            MAX(DATA_MOVTO_T) AS data_ultimo_faturamento
        FROM RVE520CSV2
        GROUP BY SUBSTRING_INDEX(TRIM(CODIGO_PRODUTO), '.', 1)
    )
    SELECT
        e.produto,
        e.descricao_material,
        e.tipo_material,
        e.quantidade_estoque,
        e.custo_total,

        ucq.data_ultima_compra,
        ucq.quantidade_compra,
        ucq.ult_preco_unit_compra,

        ra.data_primeira_requisicao,
        ra.data_ultima_requisicao,
        ra.quantidade_requisitada,
        ra.produto_of,
        ra.numero_da_of,

        COALESCE(ra.produto_of, e.produto) AS chave_venda,

        uf.data_ultimo_faturamento,

        CASE
          WHEN ucq.data_ultima_compra IS NULL OR ra.data_primeira_requisicao IS NULL THEN NULL
          ELSE DATEDIFF(DATE(ra.data_primeira_requisicao), DATE(ucq.data_ultima_compra))
        END AS dias_ult_compra_ate_primeira_req,

        CASE
          WHEN ucq.data_ultima_compra IS NULL OR ra.data_ultima_requisicao IS NULL THEN NULL
          ELSE DATEDIFF(DATE(ra.data_ultima_requisicao), DATE(ucq.data_ultima_compra))
        END AS dias_ult_compra_ate_ultima_req
    FROM estoque e
    LEFT JOIN ult_compra_qtd ucq ON ucq.produto = e.produto
    LEFT JOIN req_agg ra         ON ra.produto  = e.produto
    LEFT JOIN ult_faturamento uf ON uf.produto_venda = COALESCE(ra.produto_of, e.produto)
    {where_final}
    ORDER BY e.produto;
    """

    all_params = params + status_params
    return fetch_df(query, all_params)


# =========================================================
# CORE - ESTRUTURA ACIMA
# =========================================================
def preparar_mapas(df: pd.DataFrame):
    if df.empty:
        return set(), {}

    df = df.copy()

    for col in ["codigo_empresa", "codigo_filial", "produto", "componente"]:
        if col in df.columns:
            df[col] = df[col].apply(_norm_str)

    if "sequencia" in df.columns:
        df["sequencia"] = pd.to_numeric(df["sequencia"], errors="coerce").fillna(0).astype(int)
    else:
        df["sequencia"] = 0

    if "quantidade" in df.columns:
        df["quantidade"] = pd.to_numeric(df["quantidade"], errors="coerce")

    roots_set = set(
        df.loc[df["sequencia"] == 0, "produto"]
          .dropna()
          .astype(str)
          .map(_norm_str)
          .tolist()
    )

    rel = df.loc[df["sequencia"] != 0].copy()
    rel = rel[rel["produto"].ne("") & rel["componente"].ne("")]

    parents_map = defaultdict(list)
    campos = [
        "codigo_empresa", "codigo_filial", "produto", "sequencia", "componente", "quantidade",
        "medida_1_corte", "medida_2_corte", "nro_partes", "nro_item_desenho",
        "obs_ligacao", "qtde_referencial", "tem_regra_existencia", "perc_na_formula",
        "codigo_desenho", "ult_preco_compra", "ult_custo_medio"
    ]
    campos = [c for c in campos if c in rel.columns]

    for row in rel[campos].itertuples(index=False):
        d = row._asdict()
        child = d.get("componente", "")
        parents_map[child].append(d)

    return roots_set, parents_map


def buscar_estrutura_acima(
    componente_informado: str,
    roots_set: set[str],
    parents_map: dict[str, list[dict]],
    max_niveis: int = 10,
    limitar_trilhas: int = 4000,
):
    alvo = _norm_str(componente_informado)
    if not alvo:
        return set(), pd.DataFrame()

    q = deque()
    q.append((alvo, [alvo], 0))

    seen_states = set([(alvo, 0)])
    registros = []
    roots_encontrados = set()
    trilhas_count = 0

    while q:
        node, path_rev, depth = q.popleft()
        if depth >= max_niveis:
            continue

        pais = parents_map.get(node, [])
        if not pais:
            continue

        for rel in pais:
            pai = _norm_str(rel.get("produto", ""))
            if not pai:
                continue

            novo_path_rev = path_rev + [pai]
            novo_depth = depth + 1

            path_fmt = " < ".join(novo_path_rev)
            registros.append({
                "componente_informado": alvo,
                "componente_encontrado": node,
                "produto_pai": pai,
                "nivel_acima": novo_depth,
                "trilha": path_fmt,
                "quantidade": rel.get("quantidade", None),
                "sequencia_item": rel.get("sequencia", None),
                "qtde_referencial": rel.get("qtde_referencial", None),
                "perc_na_formula": rel.get("perc_na_formula", None),
                "ult_preco_compra": rel.get("ult_preco_compra", None),
                "ult_custo_medio": rel.get("ult_custo_medio", None),
            })

            if pai in roots_set:
                roots_encontrados.add(pai)
                trilhas_count += 1
                if trilhas_count >= limitar_trilhas:
                    break

            state = (pai, novo_depth)
            if state not in seen_states:
                seen_states.add(state)
                q.append((pai, novo_path_rev, novo_depth))

        if trilhas_count >= limitar_trilhas:
            break

    df_det = pd.DataFrame(registros)
    if not df_det.empty:
        df_det = df_det.sort_values(
            by=["nivel_acima", "produto_pai", "componente_encontrado"],
            ascending=[True, True, True],
        )

    return roots_encontrados, df_det


# =========================================================
# SESSION STATE (persistência)
# =========================================================
st.session_state.setdefault("data_loaded", False)
st.session_state.setdefault("produto_busca_fixado", "")
st.session_state.setdefault("df_det_joined", pd.DataFrame())
st.session_state.setdefault("roots_encontrados", set())
st.session_state.setdefault("roots_set", set())

# filtros aplicados (persistem entre reruns)
st.session_state.setdefault("f_aplicar_tipo", True)
st.session_state.setdefault("f_modo_tipo", "Considerar")
st.session_state.setdefault("f_tipo_sel_apl", [])

st.session_state.setdefault("f_aplicar_gp", False)
st.session_state.setdefault("f_modo_gp", "Considerar")
st.session_state.setdefault("f_gp_sel_apl", [])

st.session_state.setdefault("f_aplicar_sgp", False)
st.session_state.setdefault("f_modo_sgp", "Considerar")
st.session_state.setdefault("f_sgp_sel_apl", [])

st.session_state.setdefault("f_aplicar_lp", False)
st.session_state.setdefault("f_modo_lp", "Considerar")
st.session_state.setdefault("f_lp_sel_apl", [])

# filtros do consolidado
opts_cons = load_filter_options_consolidado()
st.session_state.setdefault("c_deposito", ["ALMOX"] if "ALMOX" in opts_cons["deposito"] else [])
st.session_state.setdefault("c_grupo", [])
st.session_state.setdefault("c_subgrupo", [])
st.session_state.setdefault("c_linha", [])
st.session_state.setdefault("c_tipo", [])
st.session_state.setdefault("c_status", [])
st.session_state.setdefault("c_saldo", False)
st.session_state.setdefault("c_sem_req", False)
st.session_state.setdefault("c_com_venda", False)

# parâmetros de busca
st.session_state.setdefault("p_max_niveis", 12)
st.session_state.setdefault("p_limitar_trilhas", 4000)


# =========================================================
# SIDEBAR: BUSCA + FILTROS (COM FORMS)
# =========================================================
with st.sidebar:
    st.header("🔎 Busca")

    with st.form("form_busca", clear_on_submit=False):
        produto_input = st.text_input("Produto (código)", value=st.session_state["produto_busca_fixado"])
        max_niveis = st.slider("Profundidade máxima", 1, 30, int(st.session_state["p_max_niveis"]))
        limitar_trilhas = st.number_input("Limite de trilhas", 100, 50000, int(st.session_state["p_limitar_trilhas"]), step=100)

        colA, colB = st.columns(2)
        with colA:
            bt_buscar = st.form_submit_button("▶️ Buscar", use_container_width=True)
        with colB:
            bt_limpar = st.form_submit_button("🔄 Limpar", use_container_width=True)

    if bt_limpar:
        st.session_state["data_loaded"] = False
        st.session_state["produto_busca_fixado"] = ""
        st.session_state["df_det_joined"] = pd.DataFrame()
        st.session_state["roots_encontrados"] = set()
        st.session_state["roots_set"] = set()

        st.session_state["f_tipo_sel_apl"] = []
        st.session_state["f_gp_sel_apl"] = []
        st.session_state["f_sgp_sel_apl"] = []
        st.session_state["f_lp_sel_apl"] = []

        st.rerun()

    if bt_buscar:
        produto_fix = _norm_str(produto_input)
        if not produto_fix:
            st.warning("Informe um produto.")
            st.stop()

        st.session_state["produto_busca_fixado"] = produto_fix
        st.session_state["p_max_niveis"] = int(max_niveis)
        st.session_state["p_limitar_trilhas"] = int(limitar_trilhas)

        with st.spinner("Carregando tabelas (ESTRUTURA / PRODUTO / RVE520CSV2)..."):
            df_estrutura = carregar_estrutura()
            df_prod = carregar_produto()
            df_ult_venda = carregar_rve520_ultima_venda()

        if df_estrutura.empty:
            st.error("Não foi possível carregar a tabela ESTRUTURA.")
            st.stop()

        roots_set, parents_map = preparar_mapas(df_estrutura)

        with st.spinner("Buscando estrutura acima..."):
            roots_encontrados, df_det = buscar_estrutura_acima(
                componente_informado=produto_fix,
                roots_set=roots_set,
                parents_map=parents_map,
                max_niveis=int(max_niveis),
                limitar_trilhas=int(limitar_trilhas),
            )

        df_det_joined = df_det.copy()
        if not df_det_joined.empty:
            df_det_joined["produto_pai"] = df_det_joined["produto_pai"].astype(str).map(_norm_str)

            if not df_prod.empty:
                df_prod2 = df_prod.copy()
                df_prod2["codigo_produto_material"] = df_prod2["codigo_produto_material"].astype(str).map(_norm_str)
                df_det_joined = df_det_joined.merge(
                    df_prod2[["codigo_produto_material", "tipo_material", "GP_codigo_grupo", "SGP_codigo_subgrupo", "LP_codigo_linha"]],
                    left_on="produto_pai",
                    right_on="codigo_produto_material",
                    how="left",
                ).drop(columns=["codigo_produto_material"], errors="ignore")

            if not df_ult_venda.empty:
                df_det_joined = df_det_joined.merge(
                    df_ult_venda,
                    left_on="produto_pai",
                    right_on="CODIGO_PRODUTO",
                    how="left",
                ).drop(columns=["CODIGO_PRODUTO"], errors="ignore")

        # salvar no state
        st.session_state["df_det_joined"] = df_det_joined
        st.session_state["roots_encontrados"] = roots_encontrados
        st.session_state["roots_set"] = roots_set
        st.session_state["data_loaded"] = True

        # opções (para filtros)
        opt_tipo = _options_from(df_det_joined, "tipo_material")
        opt_gp = _options_from(df_det_joined, "GP_codigo_grupo")
        opt_sgp = _options_from(df_det_joined, "SGP_codigo_subgrupo")
        opt_lp = _options_from(df_det_joined, "LP_codigo_linha")

        st.session_state["opt_tipo_material"] = opt_tipo
        st.session_state["opt_gp"] = opt_gp
        st.session_state["opt_sgp"] = opt_sgp
        st.session_state["opt_lp"] = opt_lp

        # DEFAULT "PA" real (só se ainda não existe seleção aplicada)
        if not st.session_state["f_tipo_sel_apl"] and "PA" in opt_tipo:
            st.session_state["f_tipo_sel_apl"] = ["PA"]
            st.session_state["f_aplicar_tipo"] = True
            st.session_state["f_modo_tipo"] = "Considerar"

        st.rerun()

    # filtros só aparecem após busca
    if st.session_state["data_loaded"]:
        st.divider()
        st.header("🧪 Filtros (aplicar manualmente)")

        opt_tipo = st.session_state.get("opt_tipo_material", [])
        opt_gp = st.session_state.get("opt_gp", [])
        opt_sgp = st.session_state.get("opt_sgp", [])
        opt_lp = st.session_state.get("opt_lp", [])

        with st.form("form_filtros", clear_on_submit=False):
            aplicar_tipo = st.checkbox("Aplicar Tipo Material", value=st.session_state["f_aplicar_tipo"])
            modo_tipo = st.radio("Modo Tipo", ["Considerar", "Desconsiderar"], horizontal=True,
                                 index=0 if st.session_state["f_modo_tipo"] == "Considerar" else 1)
            tipo_sel = st.multiselect("Tipo(s)", options=opt_tipo, default=st.session_state["f_tipo_sel_apl"])

            aplicar_gp = st.checkbox("Aplicar GP", value=st.session_state["f_aplicar_gp"])
            modo_gp = st.radio("Modo GP", ["Considerar", "Desconsiderar"], horizontal=True,
                               index=0 if st.session_state["f_modo_gp"] == "Considerar" else 1)
            gp_sel = st.multiselect("GP", options=opt_gp, default=st.session_state["f_gp_sel_apl"])

            aplicar_sgp = st.checkbox("Aplicar SGP", value=st.session_state["f_aplicar_sgp"])
            modo_sgp = st.radio("Modo SGP", ["Considerar", "Desconsiderar"], horizontal=True,
                                index=0 if st.session_state["f_modo_sgp"] == "Considerar" else 1)
            sgp_sel = st.multiselect("SGP", options=opt_sgp, default=st.session_state["f_sgp_sel_apl"])

            aplicar_lp = st.checkbox("Aplicar LP", value=st.session_state["f_aplicar_lp"])
            modo_lp = st.radio("Modo LP", ["Considerar", "Desconsiderar"], horizontal=True,
                               index=0 if st.session_state["f_modo_lp"] == "Considerar" else 1)
            lp_sel = st.multiselect("LP", options=opt_lp, default=st.session_state["f_lp_sel_apl"])
            
            aplicar_venda = st.checkbox("Aplicar filtro de Última Venda (Estrutura)",
                value=st.session_state.get("f_aplicar_venda", True)
            )
            modo_venda = st.selectbox("Filtro de Última Venda", ["Somente com venda", "Somente sem venda", "Por período"], key="modo_venda")
            venda_ini = st.date_input("Venda - Data inicial (se 'Por período')", value=date(2024, 1, 1), key="venda_ini")
            venda_fim = st.date_input("Venda - Data final (se 'Por período')", value=date.today(), key="venda_fim")
            
            st.divider()
            st.subheader("📦 Consolidado")

            dep = st.multiselect("Depósito", opts_cons["deposito"], default=st.session_state["c_deposito"])
            c_gr = st.multiselect("Grupo (estoque)", opts_cons["grupo"], default=st.session_state["c_grupo"])
            c_sg = st.multiselect("Subgrupo (estoque)", opts_cons["subgrupo"], default=st.session_state["c_subgrupo"])
            c_li = st.multiselect("Linha (estoque)", opts_cons["linha"], default=st.session_state["c_linha"])
            c_tp = st.multiselect("Tipo (estoque)", opts_cons["tipo_material"], default=st.session_state["c_tipo"])
            c_st = st.multiselect("Status (requisições)", opts_cons["status"], default=st.session_state["c_status"])
            c_saldo = st.checkbox("Somente com saldo", value=st.session_state["c_saldo"])
            c_sem_req = st.checkbox("Somente sem requisição", value=st.session_state["c_sem_req"])
            c_com_venda = st.checkbox("Somente com venda", value=st.session_state["c_com_venda"])

            bt_aplicar = st.form_submit_button("✅ Aplicar filtros", use_container_width=True)

        if bt_aplicar:
            st.session_state["f_aplicar_tipo"] = aplicar_tipo
            st.session_state["f_modo_tipo"] = modo_tipo
            st.session_state["f_tipo_sel_apl"] = tipo_sel

            st.session_state["f_aplicar_gp"] = aplicar_gp
            st.session_state["f_modo_gp"] = modo_gp
            st.session_state["f_gp_sel_apl"] = gp_sel

            st.session_state["f_aplicar_sgp"] = aplicar_sgp
            st.session_state["f_modo_sgp"] = modo_sgp
            st.session_state["f_sgp_sel_apl"] = sgp_sel

            st.session_state["f_aplicar_lp"] = aplicar_lp
            st.session_state["f_modo_lp"] = modo_lp
            st.session_state["f_lp_sel_apl"] = lp_sel

            st.session_state["c_deposito"] = dep
            st.session_state["c_grupo"] = c_gr
            st.session_state["c_subgrupo"] = c_sg
            st.session_state["c_linha"] = c_li
            st.session_state["c_tipo"] = c_tp
            st.session_state["c_status"] = c_st
            st.session_state["c_saldo"] = c_saldo
            st.session_state["c_sem_req"] = c_sem_req
            st.session_state["c_com_venda"] = c_com_venda
            # ✅ salva filtro de venda no state (senão não aplica e não persiste)
            st.session_state["f_aplicar_venda"] = aplicar_venda
            st.session_state["f_modo_venda"] = modo_venda
            st.session_state["f_venda_ini"] = venda_ini
            st.session_state["f_venda_fim"] = venda_fim
                        

            st.rerun()


# =========================================================
# CORPO: se não carregou, mostra aviso (uma vez)
# =========================================================
if not st.session_state["data_loaded"]:
    st.info("Informe um produto e clique em **Buscar** na barra lateral.")
    st.stop()

produto_busca = st.session_state["produto_busca_fixado"]
df_det_joined = st.session_state["df_det_joined"].copy()
roots_encontrados = st.session_state["roots_encontrados"]
roots_set = st.session_state["roots_set"]


# =========================================================
# RESULTADO BASE
# =========================================================
st.subheader("✅ Resultado")

c1, c2, c3 = st.columns(3)
c1.metric("Produto pesquisado", produto_busca)
c2.metric("Produtos nível 1 encontrados", f"{len(roots_encontrados):,}")
c3.metric("Relações encontradas", f"{len(df_det_joined):,}" if not df_det_joined.empty else "0")

if df_det_joined.empty and not roots_encontrados:
    st.warning("Nenhuma relação encontrada para o produto informado.")
    st.stop()

st.markdown("### 🥇 Produtos nível 1 (sequencia=0) que utilizam o componente")
roots_list = sorted(list(roots_encontrados))
st.dataframe(pd.DataFrame({"produto_nivel_1": roots_list}), use_container_width=True, height=260)


# =========================================================
# APLICA FILTROS (usando os filtros APLICADOS do session_state)
# =========================================================
df_filtrado = df_det_joined.copy()

if st.session_state["f_aplicar_tipo"]:
    incluir = st.session_state["f_tipo_sel_apl"] if st.session_state["f_modo_tipo"] == "Considerar" else None
    excluir = st.session_state["f_tipo_sel_apl"] if st.session_state["f_modo_tipo"] == "Desconsiderar" else None
    df_filtrado = _apply_inclusion_exclusion(df_filtrado, "tipo_material", incluir, excluir)

if st.session_state["f_aplicar_gp"]:
    incluir = st.session_state["f_gp_sel_apl"] if st.session_state["f_modo_gp"] == "Considerar" else None
    excluir = st.session_state["f_gp_sel_apl"] if st.session_state["f_modo_gp"] == "Desconsiderar" else None
    df_filtrado = _apply_inclusion_exclusion(df_filtrado, "GP_codigo_grupo", incluir, excluir)

if st.session_state["f_aplicar_sgp"]:
    incluir = st.session_state["f_sgp_sel_apl"] if st.session_state["f_modo_sgp"] == "Considerar" else None
    excluir = st.session_state["f_sgp_sel_apl"] if st.session_state["f_modo_sgp"] == "Desconsiderar" else None
    df_filtrado = _apply_inclusion_exclusion(df_filtrado, "SGP_codigo_subgrupo", incluir, excluir)

if st.session_state["f_aplicar_lp"]:
    incluir = st.session_state["f_lp_sel_apl"] if st.session_state["f_modo_lp"] == "Considerar" else None
    excluir = st.session_state["f_lp_sel_apl"] if st.session_state["f_modo_lp"] == "Desconsiderar" else None
    df_filtrado = _apply_inclusion_exclusion(df_filtrado, "LP_codigo_linha", incluir, excluir)
# =========================================================
# APLICA FILTRO DE ÚLTIMA VENDA (usando valores confirmados do form)
# =========================================================
if st.session_state.get("f_aplicar_venda", False) and "ultima_venda" in df_filtrado.columns:
    df_filtrado = df_filtrado.copy()
    df_filtrado["ultima_venda"] = pd.to_datetime(df_filtrado["ultima_venda"], errors="coerce")

    modo = st.session_state.get("f_modo_venda", "Somente com venda")
    ini = st.session_state.get("f_venda_ini", date(2024, 1, 1))
    fim = st.session_state.get("f_venda_fim", date.today())

    if modo == "Somente com venda":
        df_filtrado = df_filtrado[df_filtrado["ultima_venda"].notna()]
    elif modo == "Somente sem venda":
        df_filtrado = df_filtrado[df_filtrado["ultima_venda"].isna()]
    else:  # Por período
        ini_dt = pd.to_datetime(ini)
        fim_dt = pd.to_datetime(fim) + pd.Timedelta(days=1) - pd.Timedelta(seconds=1)
        df_filtrado = df_filtrado[df_filtrado["ultima_venda"].between(ini_dt, fim_dt, inclusive="both")]

# =========================================================
# CONSOLIDADO aplicado (pelo produto pesquisado / produto_trilha)
# =========================================================
st.markdown("### 🔗 Consolidado aplicado no Detalhamento (produto_trilha = produto pesquisado)")

df_det_com_cons = df_filtrado.copy()

if df_det_com_cons.empty:
    st.warning("Após filtros, não restou nenhum registro no detalhamento.")
else:
    with st.spinner(f"Carregando consolidado do produto: {produto_busca} ..."):
        df_cons = load_consolidado_por_produtos(
            produtos=[produto_busca],
            depositos=st.session_state["c_deposito"],
            grupos=st.session_state["c_grupo"],
            subgrupos=st.session_state["c_subgrupo"],
            linhas=st.session_state["c_linha"],
            tipos=st.session_state["c_tipo"],
            status_list=st.session_state["c_status"],
            somente_com_saldo=st.session_state["c_saldo"],
            somente_sem_req=st.session_state["c_sem_req"],
            somente_com_venda=st.session_state["c_com_venda"],
        )

    # garante colunas cons_* existirem sempre
    cols_cons = [
        "cons_quantidade_estoque",
        "cons_custo_total",
        "cons_data_ultima_compra",
        "cons_quantidade_compra",
        "cons_ult_preco_compra",
        "cons_data_primeira_requisicao",
        "cons_data_ultima_requisicao",
        "cons_quantidade_requisitada",
        "cons_data_ultimo_faturamento",
        "cons_dias_ult_compra_ate_primeira_req",
        "cons_dias_ult_compra_ate_ultima_req",
        "cons_produto_of",
        "cons_numero_da_of",
    ]
    for c in cols_cons:
        if c not in df_det_com_cons.columns:
            df_det_com_cons[c] = None

    if df_cons.empty:
        st.warning("Consolidado não retornou linha para este produto com os filtros atuais.")
    else:
        row = df_cons.iloc[0].to_dict()
        map_cons = {
            "quantidade_estoque": "cons_quantidade_estoque",
            "custo_total": "cons_custo_total",
            "data_ultima_compra": "cons_data_ultima_compra",
            "quantidade_compra": "cons_quantidade_compra",
            "ult_preco_unit_compra": "cons_ult_preco_compra",
            "data_primeira_requisicao": "cons_data_primeira_requisicao",
            "data_ultima_requisicao": "cons_data_ultima_requisicao",
            "quantidade_requisitada": "cons_quantidade_requisitada",
            "data_ultimo_faturamento": "cons_data_ultimo_faturamento",
            "dias_ult_compra_ate_primeira_req": "cons_dias_ult_compra_ate_primeira_req",
            "dias_ult_compra_ate_ultima_req": "cons_dias_ult_compra_ate_ultima_req",
            "produto_of": "cons_produto_of",
            "numero_da_of": "cons_numero_da_of",
        }
        for src, dst in map_cons.items():
            df_det_com_cons[dst] = row.get(src, None)

    df_det_com_cons["produto_trilha"] = produto_busca


# =========================================================
# DETALHAMENTO (colunas selecionadas)
# =========================================================
st.markdown("### 📄 Detalhamento (colunas selecionadas)")

if df_det_com_cons.empty:
    st.info("Sem dados para exibir.")
else:
    colunas_exibir = [
        "produto_pai",
        "tipo_material",
        "GP_codigo_grupo",
        "SGP_codigo_subgrupo",
        "LP_codigo_linha",
        "ultima_venda",
        "componente_encontrado",
        "nivel_acima",
        "quantidade",
        "sequencia_item",
        "trilha",
        "cons_quantidade_estoque",
        "cons_custo_total",
        "cons_data_ultima_compra",
        "cons_quantidade_compra",
        "cons_data_primeira_requisicao",
        "cons_quantidade_requisitada",
        "cons_data_ultimo_faturamento",
        "cons_dias_ult_compra_ate_primeira_req",
        "cons_dias_ult_compra_ate_ultima_req",
        "ult_preco_compra",
        "ult_custo_medio",
        "perc_na_formula",
        "qtde_referencial",
        "cons_data_ultima_requisicao",
        "cons_produto_of",
        "cons_numero_da_of",
        "produto_trilha",
    ]

    cols_present = [c for c in colunas_exibir if c in df_det_com_cons.columns]
    df_view = df_det_com_cons[cols_present].copy()

    # formata datas
    if "ultima_venda" in df_view.columns:
        df_view["ultima_venda"] = pd.to_datetime(df_view["ultima_venda"], errors="coerce").dt.strftime("%d/%m/%Y")

    for c in [
        "cons_data_ultima_compra",
        "cons_data_primeira_requisicao",
        "cons_data_ultima_requisicao",
        "cons_data_ultimo_faturamento",
    ]:
        if c in df_view.columns:
            df_view[c] = pd.to_datetime(df_view[c], errors="coerce").dt.strftime("%d/%m/%Y")

    st.dataframe(df_view, use_container_width=True, height=560)

    csv_bytes = df_view.to_csv(index=False, sep=";").encode("utf-8-sig")
    st.download_button(
        "📥 Baixar detalhamento (CSV)",
        data=csv_bytes,
        file_name=f"estrutura_acima_com_consolidado_{_norm_str(produto_busca)}.csv",
        mime="text/csv",
    )


# =========================================================
# SUNBURST (mais legível que árvore Graphviz quando tem muitos)
# =========================================================
st.markdown("---")
st.markdown("### 🍩 Sunburst (hierarquia pai → componente)")

if df_filtrado.empty:
    st.info("Sem dados após filtros para montar o Sunburst.")
else:
    df_sb = df_filtrado.copy()
    df_sb["produto_pai"] = df_sb["produto_pai"].astype(str).map(_norm_str)
    df_sb["componente_encontrado"] = df_sb["componente_encontrado"].astype(str).map(_norm_str)

    if "quantidade" in df_sb.columns:
        df_sb["quantidade"] = pd.to_numeric(df_sb["quantidade"], errors="coerce").fillna(0)
    else:
        df_sb["quantidade"] = 1

    df_sb = df_sb[(df_sb["produto_pai"] != "") & (df_sb["componente_encontrado"] != "")].copy()

    modo_valor = st.radio("Tamanho do setor (value)", ["Somar quantidade", "Contar ocorrências"], horizontal=True)
    if modo_valor == "Contar ocorrências":
        df_sb["value"] = 1
    else:
        df_sb["value"] = df_sb["quantidade"]

    # agrega (reduz volume)
    df_sb_agg = (
        df_sb.groupby(["produto_pai", "componente_encontrado"], as_index=False)["value"].sum()
    )

    top_n = st.slider("Limitar Top N componentes por pai (reduz poluição)", 10, 300, 40, step=10)

    df_sb_top = (
        df_sb_agg.sort_values(["produto_pai", "value"], ascending=[True, False])
                .groupby("produto_pai", as_index=False)
                .head(top_n)
    )

    fig = px.sunburst(
        df_sb_top,
        path=["produto_pai", "componente_encontrado"],
        values="value",
    )
    fig.update_layout(margin=dict(t=10, l=10, r=10, b=10))
    st.plotly_chart(fig, use_container_width=True)

st.caption(f"Hoje: {date.today().strftime('%d/%m/%Y')}")