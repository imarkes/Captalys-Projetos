from datetime import datetime
import numpy as np
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.functions import *

spark = SparkSession \
    .builder \
    .appName('dados_varejo') \
    .config("spark.jars", "/home/ivan/Dev/spark/jars/*.jar")\
    .getOrCreate()


# operacoes desembolsadas sao identificadas a partir da tabela de contratos em platform_cessao
df_operacoes = spark.sql("""
            SELECT con.data_cessao, cp.identificador, cd.numero_documento,
            sd.numero_documento as documento_sacado, prod.nome as produto, con.data_vencimento,
            con.valor_aquisicao, con.valor_titulo as valor_face
            FROM captalys_trusted.platform_cessao_master_contrato_processo cp
            INNER JOIN captalys_trusted.platform_cessao_master_contrato as con ON cp.uuid = con.processo_uuid
            INNER JOIN captalys_trusted.platform_cessao_master_cedente_documento as cd ON con.cedente_id = cd.cedente_id
            INNER JOIN captalys_trusted.platform_cessao_master_sacado_documento as sd ON con.sacado_id = sd.sacado_id
            INNER JOIN captalys_trusted.platform_cessao_master_produto as prod ON cp.produto_id = prod.id
            WHERE cp.produto_id in (8, 12, 33, 36, 41, 43, 44, 53, 55)
            LIMIT 5;
    """)

# dados de pagamento das operacoes em platform_cessao
df_liquidacao = spark.sql("""
            select
            lp.data_criacao,
            lp.identificador,
            lp.estado_atual,
            lp.uuid,
            lp.produto_id,
            lp.proc_id,
            lp.hash_liquidacao,
            lp.tipo_operacao,
            pag.id,
            pag.valor_pago,
            pag.valor_desconto,
            pag.data_pagamento,
            pag.data_criacao,
            pag.data_atualizacao,
            pag.parcela_id,
            pag.tipo_liquidacao,
            pag.origem_pagamento,
            pag.sem_financeiro,
            pag.liquidacao_lote_id,
            pag.liquidacao_processo_uuid,
            pag.valor_face_liquidado,
            pag.valor_nominal,
            pag.multa_cobrada_anteriormente,
            pag.valor_juros_aberto_anteriormente,
            pag.valor_multa_aberta_anteriormente,
            pag.ultima_data_cobranca_juros,
            pag.ultima_data_cobranca_juros_anteriormente,
            pag.valor_juros_moratorios,
            pag.valor_multa_moratoria,
            pag.valor_juros_aberto_liquidado,
            pag.valor_multa_aberta_liquidada,
            pag.valor_juros_liquidado,
            pag.valor_multa_liquidada
        from
            captalys_trusted.platform_cessao_master_liquidacao_processo lp
        inner join captalys_trusted.platform_cessao_master_liquidacao_pagamento as pag on
            lp.uuid = pag.liquidacao_processo_uuid
        inner join (
            select
                distinct(identificador) as identificador
            from
                captalys_trusted.platform_cessao_master_contrato_processo
            where
                produto_id in (8, 12, 33, 36, 41, 43, 44, 53, 55)
        ) cp on
            lp.identificador = cp.identificador
            LIMIT 5
            """)

print("Lendo base de operacoes")
print("Lendo base de Liquidacoes")

# Padronizacao do numero de documento df_operacoes
df_operacoes = df_operacoes \
    .withColumn('numero_documento',
                when(f.col('numero_documento') == 32402502000135,
                     f.col('documento_sacado')).otherwise(f.col('numero_documento')))

# df_operacoes = df_operacoes \
#     .withColumn('numero_documento',
#                 udf(lambda x: x["documento_sacado"] if x["numero_documento"] == 32402502000135 else x[
#                     "numero_documento"]
#                     )
#                 )

lista_operacoes = df_operacoes.drop("documento_sacado") \
    .select('identificador')\
    .withColumn('len',
                f.lit(length('identificador'))) \
    .filter(f.col('len') == 36).select(f.col('identificador')).collect()

# informacoes detalhadas sobre as operacoes desembolsadas - postgres/contratacao/propostas
df_propostas = spark \
    .sql("""
            SELECT operacao_id as identificador, id_proposta, valor_presente,
            valor_credito, prazo, prazo_max, retencao, tac, custo_fixo, custo_total,
            score, fluxo_medio_meses, valor_limite, valor_minimo, prazo_esperado,
            produto as tipo_proposta, tx_retencao_rating, faturamento_medio_sem_out,
            taxa_cap FROM captalys_trusted.private_credit_master_propostas WHERE operacao_id IN {}
        """.format(tuple(lista_operacoes))
         )

print("Lendo base contratacao - propostas")

df_ids_propostas = spark \
    .sql("""
            SELECT operacao_id as identificador, id_proposta
            FROM xxxxx
            WHERE operacao_id IN {}
        """.format(tuple(lista_operacoes))
         )

# adicionando a tabela origem de onde os dados estao sendo lidos para referencia
df_propostas = df_propostas \
    .withColumn('origem_dados',f.when(col('valor_presente').isnull('private_credit_master_propostas')).otherwise(np.nan),
                df_propostas.udf(
                    lambda x: "postgres/contratacao" if not isnan(x["valor_presente"]) else np.nan
                )) \
    .withColumn('identificador',
                lit(df_propostas.select('identificador')
                    ))

# unificando os identificadores. Uma proposta pode ser identificada por id_proposta
# ou operation_id entre as diferentes bases de dados e produtos
lista_identificadores = df_ids_propostas.select('id_proposta').collect() + lista_operacoes

# detalhes de propostas armazenados em postgres/pricing/propostas
df_propostas_2 = spark \
    .sql("""
            SELECT id_proposta, produto, cnpj, valor_maximo, valor_minimo, proposta
            FROM captalys_trusted.pricing_master_propostas WHERE id_proposta IN {}
        """.format(tuple(lista_identificadores))
         )
print("Lendo base captalys_trusted.pricing_master_propostas")

# tratamentos dos dados lidos de postgres/pricing/propostas
df_propostas_2 = df_propostas_2.join(df_ids_propostas, on='id_proposta', how='left') \
    .select('identificador').cast('string') \
    .withColumn('produto',
                udf(lambda x: "paypal" if x["produto"] == "fixo" else x["produto"],
                    )
                )

# separacao por produto para fazer os tratamentos especificos para padronizar a informacao
propostas_movile = df_propostas_2.filter(f.col('produto') == 'movile')
propostas_tomatico_fixo = df_propostas_2.filter(f.col("produto") == "tomatico_fixo")
propostas_b2w_fixo = df_propostas_2.filter(f.col("produto") == "b2w_fixo")
propostas_b2w_variavel = df_propostas_2.filter(f.col("produto") == "b2w_variavel")
propostas_paypal = df_propostas_2.filter(f.col("produto") == "paypal")

# os detalhesde cada proposta estao armazenados em um json que sera processado em variaveis
detalhes_movile = spark.createDataFrame(propostas_movile["proposta"].collect())
detalhes_tomatico_fixo = spark.createDataFrame(propostas_tomatico_fixo["proposta"].collect())
detalhes_b2w_fixo = spark.createDataFrame(propostas_b2w_fixo["proposta"].collect())
detalhes_b2w_variavel = spark.createDataFrame(propostas_b2w_variavel["proposta"].collect())
detalhes_paypal = spark.createDataFrame(propostas_paypal["proposta"].collect())

# Merge dos dataframes
detalhes_tomatico_fixo = propostas_tomatico_fixo.join(detalhes_tomatico_fixo, on="id_proposta")

# Cria a colunas novas
detalhes_tomatico_fixo = detalhes_tomatico_fixo \
    .withColumn("identificador",
                detalhes_tomatico_fixo.select("id_proposta")) \
    .withColumn("taxa", lit(detalhes_tomatico_fixo.filter(detalhes_tomatico_fixo["taxa"] / 100))) \
    .drop('devido')\
    .drop('pagamento') \
    .join(spark.createDataFrame(detalhes_tomatico_fixo.select("detalhe").collect())) \
    .withColumn("score", lit(udf(lambda x: x['risco']['score']))) \
    .join(spark.createDataFrame(detalhes_tomatico_fixo.select("dados_financeiros").collect())) \
    .withColumn("prazo", lit(udf(lambda x: (x['pagamento']['padrao']['quantidade']) / 4)))

# Renomeia as colunas
detalhes_tomatico_fixo = detalhes_tomatico_fixo \
    .withColumnRenamed("tipo_proposta", "fixo") \
    .withColumnRenamed("periodicidade", "semanal") \
    .withColumnRenamed("produto", "tomatico_fixo") \
    .withColumnRenamed("referencia_pricing", "fat_media") \
    .withColumnRenamed("valor_presente", "valor_escolhido") \
    .select(["identificador", "id_proposta", "cnpj", "produto",
             "valor_maximo", "valor_minimo", "valor_escolhido",
             "valor_credito", "prazo", "taxa", "custo_fixo",
             "custo_processamento", "custo_total", "score",
             "meses_analisados", "fat_media", "tipo_proposta",
             "periodicidade"]) \
    .withColumn("origem_dados", f.lit("captalys_trusted.pricing_master"))

detalhes_movile = detalhes_movile \
    .drop("data_limite_contrato") \
    .drop("parcelas") \
    .join(spark.createDataFrame(detalhes_movile.select("detalhe").collect())) \
    .withColumn("score", lit(detalhes_movile.udf(lambda x: x['risco']['score']))) \
    .join(spark.createDataFrame(detalhes_movile["dados_financeiros"].collect())) \
    .withColumnRenamed("num_parcelas", "prazo") \
    .withColumnRenamed("referencia_pricing", "fat_media")

detalhes_movile = propostas_movile.join(detalhes_movile, on="id_proposta")

detalhes_movile = detalhes_movile \
    .withColumn(("tipo_proposta", f.lit("fixo")), ("periodicidade", f.lit("mensal"))) \
    .select("identificador", "id_proposta", "cnpj", "produto", "valor_maximo",
             "valor_minimo", "valor_escolhido", "valor_credito", "prazo", "taxa",
             "custo_fixo", "custo_processamento", "custo_total", "score",
             "meses_analisados", "fat_media", "tipo_proposta", "periodicidade") \
    .withColumn("origem_dados", f.lit("captalys_trusted.pricing_master_"))

detalhes_paypal = detalhes_paypal.join(propostas_paypal, on="id_proposta") \
    .join(spark.createDataFrame(detalhes_paypal.select("detalhe").collect())
          .select("valor_presente", "valor_credito", "custo_processamento", "custo_fixo", "custo_total")) \
    .join(spark.createDataFrame(detalhes_paypal.select("dados_financeiros").collect())) \
    .withColumn("score", f.lit(udf(lambda x: x['risco']['score']))) \
    .withColumn("taxa", f.lit(detalhes_paypal["taxa"] / 100)) \
    .withColumnRenamed("valor_presente", "valor_escolhido") \
    .withColumnRenamed("fat_medio_bruto", "fat_media") \
    .withColum("tipo_proposta", f.lit("fixo")) \
    .withColum("periodicidade", f.lit("diaria")) \
    .select("identificador", "id_proposta", "cnpj", "produto", "valor_maximo",
             "valor_minimo", "valor_escolhido", "valor_credito", "prazo", "taxa",
             "custo_fixo", "custo_processamento", "custo_total", "score",
             "meses_analisados", "fat_media", "tipo_proposta", "periodicidade")

docs_paypal = df_operacoes \
    .filter(f.col("identificador").isin(detalhes_paypal["identificador"].collect())) \
    .select("identificador", "numero_documento") \
    .withColumnRenamed("numero_documento", "cnpj")

# VERIFICAR ###########################################################
docs_paypal.set_index("identificador", inplace=True)
detalhes_paypal.set_index("identificador", inplace=True)
detalhes_paypal.update(docs_paypal)
detalhes_paypal.reset_index(inplace=True)

detalhes_paypal = detalhes_paypal.withColumn("origem_dados", f.lit("captalys_trusted.pricing_master_"))

detalhes_b2w_fixo = detalhes_b2w_fixo.join(propostas_b2w_fixo, on="id_proposta") \
    .withColumn("prazo", f.lit(detalhes_b2w_fixo.filter(detalhes_b2w_fixo["prazo"] / 2))) \
    .withColumn("meses_analisados", f.lit(np.nan)) \
    .withColumnRenamed("valor_presente", "valor_escolhido") \
    .withColumnRenamed("taxa_mensal", "taxa") \
    .withColumnRenamed("faturamento_referencia", "fat_media") \
    .withColumn("tipo_proposta", f.lit("fixo")) \
    .withColumn("periodicidade", f.lit("quinzenal")) \
    .select("identificador", "id_proposta", "cnpj", "produto", "valor_maximo",
             "valor_minimo", "valor_escolhido", "valor_credito", "prazo", "taxa",
             "custo_fixo", "custo_processamento", "custo_total", "score",
             "meses_analisados", "fat_media", "tipo_proposta", "periodicidade") \
    .withColumn("origem_dados", f.lit("captalys_trusted.pricing_master_"))

detalhes_b2w_variavel = detalhes_b2w_variavel \
    .drop("valor_minimo") \
    .join(propostas_b2w_variavel, on="id_proposta") \
    .withColumn("custo_processamento",
                f.lit(detalhes_b2w_variavel.select("custo_total") - detalhes_b2w_variavel.select("custo_fixo"))) \
    .withColumn("valor_escolhido", f.lit(detalhes_b2w_variavel.select("valor_credito"))) \
    .withColumn("meses_analisados", f.lit(np.nan)) \
    .withColumnRenamed("taxa_esperada", "taxa") \
    .withColumnRenamed("faturamento_referencia", "fat_media") \
    .withColumn("prazo", f.lit(detalhes_b2w_variavel["prazo"] / 2)) \
    .withColumn("prazo_max", f.lit(detalhes_b2w_variavel["prazo_max"] / 2)) \
    .withColumn("tipo_proposta", f.lit("variavel")) \
    .withColumn("periodicidade", f.lit(np.nan)) \
    .select("identificador", "id_proposta", "cnpj", "produto",
             "valor_maximo", "valor_minimo", "valor_escolhido",
             "valor_credito", "prazo", "prazo_max", "taxa",
             "tx_retencao", "tx_retencao_rating", "custo_fixo",
             "custo_processamento", "custo_total", "score",
             "meses_analisados", "fat_media", "tipo_proposta",
             "periodicidade") \
    .withColumn("origem_dados", f.lit("captalys_trusted.pricing_master_"))

# Adicionando dados de propostas de captalys_trusted.pricing_clj_master -- novo servico de precificacao
propostas_clj = spark.sql("""
            SELECT ofr_tx_operation_id as identificador, *
            FROM  captalys_trusted.pricing_clj_master_offer
            WHERE ofr_tx_operation_id IN {}
            AND ofr_lg_confirmed = True

    """.format(tuple(lista_operacoes)))
print("Lendo base captalys_trusted.pricing_clj_master_")
print(propostas_clj.shape)

propostas_clj_2 = spark.sql("""
               SELECT ofr_tx_offer_id as identificador, *
               FROM captalys_trusted.pricing_clj_master_offer
               WHERE ofr_tx_offer_id IN {}
               AND ofr_lg_confirmed = True
       """.format(tuple(lista_operacoes)))
print(propostas_clj_2.shape)

# Concatenando os DF's
propostas_clj = propostas_clj.union(propostas_clj_2) \
    .withColumn("identificador", f.lit(propostas_clj.select("identificador").cast('String'))) \
    .withColumn("ofr_tx_operation_id", f.lit(propostas_clj.select("ofr_tx_operation_id").cast('String'))) \
    .withColumn("ofr_tx_offer_id", f.lit(propostas_clj.select("ofr_tx_offer_id").cast('String')))

detalhes_propostas_clj = spark.createDataFrame(propostas_clj.select("ofr_js_offer_raw").collect()) \
    .withColumnRenamed("offer_key", "ofr_tx_offer_id")

detalhes_propostas_clj = detalhes_propostas_clj.join(propostas_clj, on="ofr_tx_offer_id")

# Adicionando dados da Politica de Credito de captalys_trusted.pricing_clj_master -- novo servico de precificacao
credit_clj = spark.sql("""
        SELECT crd_tx_operation_id as ofr_tx_operation_id, crd_vl_max_limit, crd_vl_min_limit,
        crd_vl_estimated_revenue, crd_js_limit_raw
        FROM captalys_trusted.pricing_clj_master_credit
        WHERE crd_tx_operation_id IN {}
        AND crd_lg_cancelled = False
        """.format(tuple(detalhes_propostas_clj["ofr_tx_operation_id"].tolist())))

credit_clj = credit_clj \
    .withColumn("ofr_tx_operation_id", f.lit(credit_clj.select("ofr_tx_operation_id"))) \
    .filter(credit_clj["crd_vl_max_limit"] > 0)


# Funcao auxiliar para pegar o score do json com dados da politica
def get_score(dict_infos):
    try:
        score = dict_infos["info"]["external_sources"][-3]["data"]["revenue_score"]
    except:
        try:
            score = eval(
                dict_infos["info"]["external_sources"][-3]["data"]["data"]["dados"]["captalys"]["greg"]["execute"][
                    "payload"])["response"]["revenue_score"]
        except:
            score = np.nan
    return score


credit_clj = credit_clj \
    .withColumn("score",
                f.lit(credit_clj.udf(lambda x: get_score(x["crd_js_limit_raw"]), axis=1))) \
    .drop_duplicates(["ofr_tx_operation_id"])

detalhes_propostas_clj = detalhes_propostas_clj \
    .join(credit_clj, on="ofr_tx_operation_id", how="left") \
    .withColumnRenamed("ofr_tx_offer_id", "id_proposta") \
    .withColumnRenamed("crd_vl_max_limit", "valor_maximo") \
    .withColumnRenamed("crd_vl_min_limit", "valor_minimo") \
    .withColumnRenamed("disbursed_issue_amount", "valor_escolhido") \
    .withColumnRenamed("issue_amount", "valor_credito") \
    .withColumnRenamed("pmt_number", "prazo") \
    .withColumnRenamed("monthly_interest_rate", "taxa") \
    .withColumnRenamed("total_pre_fixed_amount", "custo_fixo") \
    .withColumnRenamed("contract_fee_amount", "custo_processamento") \
    .withColumnRenamed("total_pre_fixed_amount", "custo_total") \
    .withColumnRenamed("crd_vl_estimated_revenue", "fat_media") \
    .withColumnRenamed("ofr_tx_payment_type", "tipo_proposta") \
    .withColumnRenamed("payment_periodicity", "periodicidade") \
    .select("identificador", "id_proposta", "valor_maximo",
             "valor_minimo", "valor_escolhido", "valor_credito",
             "prazo", "taxa", "custo_processamento",
             "custo_total", "score", "fat_media", "tipo_proposta",
             "periodicidade") \
    .withColumn("tipo_proposta", lit(detalhes_propostas_clj.apply(
        lambda x: "fixo" if x["tipo_proposta"] == "fixed" else x["tipo_proposta"], axis=1)))


def converte_periodicidade(periodicidade):
    if periodicidade == "monthly":
        return 30
    elif periodicidade == "weekly":
        return 7
    else:
        return periodicidade


detalhes_propostas_clj = detalhes_propostas_clj\
    .withColumn("periodicidade", lit(detalhes_propostas_clj.apply(
        lambda x: converte_periodicidade(x["periodicidade"]), axis=1))) \
    .withColumn("origem_dados", lit("captalys_trusted.pricing_clj_master_"))

# Consolidando os dados
propostas_v3 = detalhes_propostas_clj \
    .union(detalhes_movile) \
    .union(detalhes_tomatico_fixo) \
    .union(detalhes_paypal) \
    .union(detalhes_b2w_fixo) \
    .union(detalhes_b2w_variavel) \
    .withColumnRenamed("valor_escolhido", "valor_presente") \
    .withColumnRenamed("tx_retencao", "retencao")

df_propostas = df_propostas \
    .withColumn("periodicidade", lit(np.nan)) \
    .withColumnRenamed("fluxo_medio_meses", "meses_analisados") \
    .withColumnRenamed("taxa_cap", "taxa") \
    .withColumnRenamed("faturamento_medio_sem_out", "fat_media") \
    .withColumnRenamed("tac", "custo_processamento") \
    .withColumnRenamed("valor_limite", "valor_maximo") \
    .withColumn("retencao", lit(udf(lambda x: np.nan if x["tipo_proposta"] == "fixo" else x["retencao"]
                                    )))

# VERIFICAR ##############
df_operacoes = df_operacoes.join(df_propostas, on="identificador", how="left")
df_operacoes.set_index("identificador", inplace=True)
propostas_v3.set_index("identificador", inplace=True)
df_operacoes.update(propostas_v3.drop(columns=["cnpj", "produto"]), overwrite=True)
propostas_v3.reset_index(inplace=True)
df_operacoes.reset_index(inplace=True)

# Adicionando dados de mysql/apiPricing
propostas_v1 = spark.sql("""
                SELECT id_proposta as identificador, propostas, parametros_fixos
                FROM XXXXXXapiPricing.propostas_rawXXX WHERE id_proposta IN {}
                """.format(tuple(df_operacoes["identificador"].tolist())))
print("Lendo base mysql/apiPricing")
print(propostas_v1.shape)

propostas_v1 = propostas_v1 \
    .withColumn("propostas", lit(udf(
        lambda x: eval(x["propostas"].replace("false", "0").replace("true", "1").replace("null", "0"))))) \
    .withColumn("parametros_fixos", lit(propostas_v1.udf(lambda x: eval(x["parametros_fixos"])))) \
    .witColumn("valor_presente", lit(propostas_v1.udf(lambda x: x["parametros_fixos"]["volume_escolhido"])))

try:
    propostas_v1 = propostas_v1 \
        .withColumn("valor_credito", lit(propostas_v1.apply(lambda x: x["propostas"]["valor_credito"])))
except:
    propostas_v1 = propostas_v1 \
        .withColumn("valor_credito", lit(propostas_v1.apply(lambda x: x["parametros_fixos"]["valor_credito"])))

propostas_v1 = propostas_v1 \
    .withColumn("prazo", lit(propostas_v1.apply(lambda x: x["propostas"]["prazo"]))) \
    .withColumn("prazo_max", lit(propostas_v1.apply(lambda x: x["propostas"]["prazo_max"]))) \
    .withColumn("retencao", lit(propostas_v1.apply(lambda x: x["propostas"]["tx_retencao"]))) \
    .withColumn("tx_retencao_rating", lit(propostas_v1.apply(lambda x: x["parametros_fixos"]["tx_retencao_rating"]))) \
    .withColumn("custo_processamento", lit(propostas_v1.apply(lambda x: x["parametros_fixos"]["custo_processamento"]))) \
    .withColumn("custo_fixo", lit(propostas_v1.apply(lambda x: x["propostas"]["custo_fixo"]))) \
    .withColumn("custo_total", lit(propostas_v1.apply(lambda x: x["propostas"]["custo_total"]))) \
    .withColumn("score", lit(propostas_v1.apply(lambda x: x["parametros_fixos"]["score"]))) \
    .withColumn("meses_analisados", lit(propostas_v1.apply(lambda x: x["parametros_fixos"]["meses_usado_fluxo_media"]))) \
    .withColumn("valor_maximo", lit(propostas_v1.apply(lambda x: x["propostas"]["valor_limite"]))) \
    .withColumn("valor_minimo", lit(propostas_v1.apply(lambda x: x["propostas"]["valor_minimo"]))) \
    .withColumn("prazo_esperado", lit(propostas_v1.apply(lambda x: x["parametros_fixos"]["prazo_esperado"]))) \
    .withColumn("taxa", lit(propostas_v1.apply(lambda x: x["parametros_fixos"]["tx_captalys_desejada"]))) \
    .withColumn("fat_media", lit(propostas_v1.apply(lambda x: x["parametros_fixos"]["faturamento_medio_sem_out"]))) \
    .withColumn("tipo_proposta", lit('variavel')) \
    .drop("propostas") \
    .drop("parametros_fixos") \
    .withColumn("origem_dados", lit("trusted/apiPricing"))  # VERIFICAR O SQL

df_operacoes.set_index("identificador", inplace=True)
propostas_v1.set_index("identificador", inplace=True)
df_operacoes.update(propostas_v1, overwrite=False)
df_operacoes.reset_index(inplace=True)
propostas_v1.reset_index(inplace=True)

df_operacoes = df_operacoes.drop("len") \
    .withColumn("data_cessao", lit(df_operacoes.apply(lambda x: x["data_cessao"].date()))) \
    .withColumn("data_vencimento", lit(df_operacoes.apply(lambda x: x["data_vencimento"].date()))) \
    .withColumn("numero_documento", lit(df_operacoes.apply(lambda x: str(x["numero_documento"]).zfill(14)))) \
    .withColumn("tipo_proposta", lit(
        udf(lambda x: "fixo" if (df_operacoes.isnull(
            x["tipo_proposta"]) and x["produto"] in ['TOMATICO FIXO', 'B2W PMT FIXA', 'PAYPAL', 'BNDES',
                                                 'MOVILEPAY']) else x["tipo_proposta"]))) \
    .withColumn("tipo_proposta", lit(udf(
        lambda x: "variavel" if df_operacoes.isnull(x["tipo_proposta"]) else x["tipo_proposta"]))) \
    .withColumn("data_referencia", lit(datetime.now().date())) \
    .drop("valor_credito") \
    .withColumn("prazo_esperado", lit(df_operacoes.apply(
        lambda x: x["prazo"] if np.isnan(x["prazo_esperado"]) else x["prazo_esperado"]))) \
    .drop("prazo") \
    .drop("prazo_max")

# Estes ids operacao estao duplicados em diferentes produtos, mas apenas uma operacao foi desembolsada
df_operacoes = df_operacoes \
    .filter(~(df_operacoes["identificador"] == '18944ef7-e065-46aa-830f-6d2f499b4bc7') &
            (df_operacoes["produto"] == "TOMATICO-VARIAVEL")) \
    .filter(~(df_operacoes["identificador"] == '0021365e-0248-40ef-b57e-1811dd56bdef') &
            (df_operacoes["produto"] == "TOMATICO-VARIAVEL"))

# Tratando dados de pagamento
df_pagamento = df_liquidacao.select("identificador", "data_pagamento", "valor_face_liquidado") \
    .join(df_operacoes.select("identificador", "numero_documento", "id_proposta"), on="identificador", how="left") \
    .withColumn("data_referencia", lit(datetime.now().date()))

# Pagamento desta operacao duplicada na base de propostas tambem esta duplicada nos dados de liquidacao

df_aux = df_pagamento.filter(df_pagamento["identificador"] == '18944ef7-e065-46aa-830f-6d2f499b4bc7') \
    .drop_duplicates()

df_pagamento = df_pagamento.filter(df_pagamento["identificador"] != '18944ef7-e065-46aa-830f-6d2f499b4bc7')

df_pagamento = df_pagamento.union(df_aux)

# A periodicidade sera expressa em dias
dict_periodicidade = {'B2W': 15,
                      'B2W PMT FIXA': 15,
                      'PAYPAL': 1,
                      'TOMATICO-VARIAVEL': 30,
                      'TOMATICO FIXO': 7,
                      'ALELO': 30,
                      'MOVILEPAY': 30,
                      'BNDES': 7,
                      'REDE': 30}

df_operacoes = df_operacoes.withColumn("periodicidade",
                                       lit(df_operacoes.apply(lambda x: dict_periodicidade[x["produto"]])))

df_operacoes = df_operacoes\
    .withColumnRenamed("tx_retencao_rating", "retencao_max")\
    .withColumnRenamed("valor_maximo", "limite_credito_max")\
    .withColumnRenamed("valor_minimo", "limite_credito_min")\
    .withColumnRenamed("valor_presente", "valor_escolhido")\
    .withColumnRenamed("retencao", "retencao_contratual")\
    .withColumnRenamed("taxa", "taxa_juros_esperada_mensal")\
    .withColumnRenamed("meses_analisados", "qtd_meses_faturamento")\
    .withColumnRenamed("fat_media", "faturamento_medio")


def monta_query(data):
    data = {k: None if type(val) == float and np.isnan(val) else val for k, val in data.items()}
    query = """INSERT INTO carteira_varejo
               (identificador, id_proposta, tipo_proposta, produto, numero_documento, score, retencao_max,
               limite_credito_max, limite_credito_min, data_cessao, data_vencimento, valor_escolhido,
               valor_aquisicao, valor_face, retencao_contratual, prazo_esperado, taxa_juros_esperada_mensal,
               custo_processamento, custo_fixo, custo_total, qtd_meses_faturamento, faturamento_medio,
               origem_dados, periodicidade, data_referencia)
               VALUES (
               '{}', '{}', '{}', '{}', '{}', {}, {}, {}, {}, '{}', '{}', {}, {}, {}, {}, {}, {}, {},
               {}, {}, {}, {}, '{}', {}, '{}')
               """.format(
        data["identificador"],
        data["id_proposta"],
        data["tipo_proposta"],
        data["produto"],
        data["numero_documento"],
        data["score"],
        data["retencao_max"],
        data["limite_credito_max"],
        data["limite_credito_min"],
        data["data_cessao"],
        data["data_vencimento"],
        data["valor_escolhido"],
        data["valor_aquisicao"],
        data["valor_face"],
        data["retencao_contratual"],
        data["prazo_esperado"],
        data["taxa_juros_esperada_mensal"],
        data["custo_processamento"],
        data["custo_fixo"],
        data["custo_total"],
        data["qtd_meses_faturamento"],
        data["faturamento_medio"],
        data["origem_dados"],
        data["periodicidade"],
        data["data_referencia"]
    )
    query = query.replace("None", "NULL")
    return query


# ##################VERIFICAR ###############################################
df_operacoes.index = list(range(len(df_operacoes)))
df_operacoes.to_excel("base_operacoes_{}.xlsx".format(datetime.now().date()))

query = """
        TRUNCATE carteira_varejo
        """
print("Truncando base anterior")
truncate(query, session_creditmodel)

print("Salvando base de Propostas")

for idx in range(len(df_operacoes)):
    try:
        data = df_operacoes.query("index=={}".format(idx)).to_dict("records")[0]
        query = monta_query(data)
        save(query, session_creditmodel)
    except:
        print(idx)
print("FIM!")

# truncando a base anterior
print("Truncando base Liquidacao")
query = """
    TRUNCATE carteira_varejo_liquidacao
    """
truncate(query, session_creditmodel)

# salvando base de liquidacao
insert_liquidacao = """
    INSERT INTO carteira_varejo_liquidacao
    (identificador, id_proposta, numero_documento, data_pagamento, valor_face_liquidado, data_referencia)
    VALUES
    ('{}', '{}', '{}', '{}', {}, '{}')
    """

df_pagamento.index = list(range(len(df_pagamento)))
print("Salvando base Liquidacao")
print(len(df_pagamento))
for idx in range(len(df_pagamento)):
    data = df_pagamento.query("index=={}".format(idx)).to_dict("records")[0]
    data = {k: None if type(v) == float and np.isnan(v) else v for k, v in data.items()}
    query = insert_liquidacao.format(
        data["identificador"],
        data["id_proposta"],
        data["numero_documento"],
        data["data_pagamento"],
        data["valor_face_liquidado"],
        data["data_referencia"]
    )
    query = query.replace("None", "NULL")
    save(query, session_creditmodel)
print("SUCESSO")
return