from functools import lru_cache
from typing import Dict, List, Optional, Tuple

import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.session import SparkSession

from .catalog import BUCKETS
from .catalog import ENVIRONMENTS
from .catalog import ENVIRONMENT_TABLES
from .catalog import EXTERNAL_TABLES
from .catalog import PREFIXOS
from .catalog import USER_MAP


class DataUtils:
    """
    O DataUtils é uma classe que abstrai diferentes ambientes de bases de dados 
    e permite utilização da mesma sintaxe para leitura e escritas de dados que visam
    faciliar o teste de um mesmo código em um mesmo ambiente
    
    Para permitir que dois usuários trabalhem no mesmo bloco de modelagem, o DataUtils
    pode ser instanciado para um ambiente de usuário, para o qual deve-se passar o nome
    do usuário executando a ferramenta
    """
        
    def __init__(
        self, 
        environment: str = "USER",
        use_external_from: str = "TEST",
        user_name: Optional[str] = None,
    ) -> None:
        """
        Instancia a classe de DataUtils
        
        :param spark: sessão spark instanciada
        :param environment: ambiente a ser criado
        :param use_external_from: para o ambiente de usuário, qual ambiente de tabelas externas referenciar
        :param user_name: nome do usuário a ser utilizado
        """
        # cria a sessão
        self.spark = SparkSession.builder.getOrCreate()

        # ajusta configurações do spark para carregamento de tabelas
        self.spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInRead", "CORRECTED")
        self.spark.conf.set("spark.sql.legacy.parquet.int96RebaseModeInWrite", "CORRECTED")
        self.spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInRead", "CORRECTED")
        self.spark.conf.set("spark.sql.legacy.parquet.datetimeRebaseModeInWrite", "CORRECTED")
        self.spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
        self.spark.conf.set("hive.exec.dynamic.partition", "true")
        self.spark.conf.set("hive.exec.dynamic.partition.mode", "nonstrict")
        self.spark.conf.set("mapred.map.tasks.speculative.execution", "false")
        self.spark.conf.set("mapred.reduce.tasks.speculative.execution", "false")
        self.spark.conf.set("spark.sql.broadcastTimeout", "10800")

        # mapeia o schema a ser utilizado
        if environment.startswith("USER_"):
            environment, use_external_from, user_name = environment.split("_")
            reverse_user_map = dict(zip(USER_MAP.values(), USER_MAP.keys()))
            user_name = reverse_user_map[user_name.lower()]
        
        self._environment = environment
        self._schema = ENVIRONMENTS[environment]
        if environment == "USER":
            assert user_name is not None, "Você precisa de um usuário mapeado"
            assert user_name in USER_MAP, "Adicione o usuário ao mapa de usuários"
            self.external_catalog = EXTERNAL_TABLES[use_external_from]
            self._prefixos = {"external": PREFIXOS[use_external_from]["external"], "internal": f"{USER_MAP[user_name]}_"}
        else:
            self._prefixos = PREFIXOS[environment]
            self.external_catalog = EXTERNAL_TABLES[environment]
        self.internal_catalog = {tn: f"{self._schema}.{tp}" for tn, tp in ENVIRONMENT_TABLES.items()}

        # seleciona o bucket
        if environment == "USER":
            self.bucket = f"{BUCKETS['USER']}/{USER_MAP[user_name]}/data"
        else:
            self.bucket = BUCKETS[environment]

        # seleciona a pasta de checkpoint
        try:
            self.spark.sparkContext.setCheckpointDir(f"{self.bucket}/checkpoint")
        except:
            self.spark.sparkContext.setCheckpointDir("tmp")

    def create_glue_catalog(self, catalog_name: str) -> None:
        """
        Função utilizada para criar um catálogo no Glue

        :param catalog_name (str): O nome do catálogo
        """
        self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {catalog_name}")

    def get_glue_catalogs(self) -> List[str]:
        """
        Busca a lista de catálogos do Glue

        :return: A lista de catálogos
        """
        return [r[0] for r in self.spark.sql("SHOW DATABASES").collect()]
    
    def get_tables_in_catalog(self, catalog_name: str) -> List[str]:
        """
        Busca a lista de tabelas em um catálogo do Glue

        :param catalog_name (str): O nome do catálogo
        :return: A lista de tabelas
        """
        return [r[1] for r in self.spark.sql(f"SHOW TABLES IN {catalog_name}").collect()]
    
    def get_tables_in_schema(self) -> List[str]:
        """
        Lista as tabelas contidas no schema do ambiente

        :return: lista de tabelas
        """
        return self.get_tables_in_catalog(self._schema)

    def delete_glue_catalog(self, catalog_name: str) -> None:
        """
        Funcao utilizada para deletar um catalogo no Glue
        
        :param catalog_name (str): O nome do catalogo
        """
        assert (
            catalog_name in USER_MAP.values()
            or catalog_name.startswith("ow")
        ), "Você não pode deletar um catalogo que não seja de usuário"
        try:
            self.spark.sql(f"DROP DATABASE {catalog_name}")
        except Exception as e:
            print(f"Error deleting catalog: {e}")
    
    def list_available_tables(self) -> List[Tuple[str, str]]:
        """
        Lista todas as tabelas disponíveis que foram mapeadas no catalogo
        
        :return: lista de tuplas de catalogo e nome da tabela
        """
        # mapeia as bases que existem em cada schema
        schema_map = dict()
        for (catalog, table_name, table_path) in (
            [
                ("internal", tn, tp.split(".")[0] + "." + self._prefixos["internal"] + tp.split(".")[1]) 
                for tn, tp in self.internal_catalog.items()
            ] 
            + [
                ("external", tn, tp.split(".")[0] + "." + self._prefixos["external"] + tp.split(".")[1])
                if tp.split(".")[0] == self._schema
                else ("external", tn, tp)
                for tn, tp in self.external_catalog.items()
            ]
        ):
            schema, table_wo_schema = table_path.split(".")
            if schema not in schema_map:
                schema_map[schema] = list()
            schema_map[schema].append((catalog, table_name, table_wo_schema))

        # percorre os schemas e extrai as tabelas disponíveis
        available_tables = list()
        for schema in schema_map:
            table_list = self.get_tables_in_catalog(schema)
            for catalog, table, table_name in schema_map[schema]:
                if table_name in table_list:
                    available_tables.append((catalog, table))
        
        return available_tables
    
    def _get_table_name(self, alias: str, catalog: str) -> str:
        """
        Obtem o nome da tabela a associada a um alias e catalog

        :param alias (str): Alias usado no registro da tabela no arquivo de bases
        :param catalog (str): nome do catalog que retem a base de dados (infer, external, internal)
        :return: nome da base no glue
        """
        if catalog == "infer":
            if alias in self.external_catalog and alias in self.internal_catalog:
                raise ValueError("Não é impossível inferir o catalog, dado que a base está em ambos")
            elif alias in self.external_catalog:
                catalog = "external"
            else:
                catalog = "internal"
        else:
            if catalog != "external" and catalog != "internal":
                raise ValueError("Selecione um catalogo válido - infer, external, internal")
        
        alias = self.external_catalog[alias] if catalog == "external" else self.internal_catalog[alias]
        schema, name = alias.split(".")
        
        if schema == self._schema:
            return f"{schema}.{self._prefixos[catalog]}{name}"
        else:
            return f"{schema}.{name}"
    
    def is_table_available(self, alias: str, catalog: str = "infer") -> bool:
        """
        Retorna verdadeiro se a tabela existir

        :param alias (str): Alias usado no registro da tabela no arquivo de bases
        :param catalog (str): nome do catalog que retem a base de dados (infer, external, internal)
        :return: True/False
        """
        table_name = self._get_table_name(alias, catalog)
        try:
            response = self.spark.sql(f"DESCRIBE {table_name}")
            return True
        except Exception as e:
            return False
        
    def _get_partition_columns(self, catalog_name: str, table_name: str) -> List[str]:
        """
        Busca as colunas de partição de uma tabela

        :param catalog_name (str): O nome do catálogo
        :param table_name (str): O nome da tabela
        :return: A lista de colunas de partição
        """
        describe = self.spark.sql(f"DESCRIBE TABLE EXTENDED {catalog_name}.{table_name}").collect()
        get_partitions = False
        partition_cols = list()
        for c in describe:
            if c[0] == "# Partition Information":
                get_partitions = True
                continue
            if get_partitions and c[0] == "# Detailed Table Information":
                break
            elif get_partitions and c[0] != "# col_name" and c[0] != "":
                partition_cols.append(c[0])
        return partition_cols

    def get_partition_columns(self, alias: str, catalog: str = "infer") -> List[str]:
        """
        Busca as colunas de partição de uma tabela

        :param alias (str): Alias usado no registro da tabela no arquivo de bases
        :param catalog (str): nome do catalog que retem a base de dados (infer, external, internal)
        :return: A lista de colunas de partição
        """
        table_name = self._get_table_name(alias, catalog)
        schema, name = table_name.split(".")
        return self._get_partition_columns(schema, name)
    
    @lru_cache(maxsize=32)
    def _get_partition_values(self, catalog_name: str, table_name: str) -> List[Dict[str, str]]:
        """
        Busca os valores de partição associados a cada coluna

        :param catalog_name (str): O nome do catálogo
        :param table_name (str): O nome da tabela
        :return: A lista de valores de partição
        """
        try:
            parts = self.spark.sql(f"SHOW PARTITIONS {catalog_name}.{table_name}").collect()
            partitions = list()
            for p in parts:
                to_add = dict()
                for d in p[0].split("/"):
                    k, v = d.split("=")
                    to_add[k] = v
                partitions.append(to_add)
        except:
            partition_cols = self._get_partition_columns(catalog_name, table_name)
            cols = self.spark.sql(f"SELECT DISTINCT {','.join(partition_cols)} FROM {catalog_name}.{table_name}").collect()
            partitions = [dict(zip(partition_cols, c)) for c in cols]
        
        return partitions
    
    def get_partition_values(self, alias: str, catalog: str = "infer") -> List[Dict[str, str]]:
        """
        Busca os valores de partição associados a cada coluna

        :param alias (str): Alias usado no registro da tabela no arquivo de bases
        :param catalog (str): nome do catalog que retem a base de dados (infer, external, internal)
        :return: A lista de valores de partição
        """
        table_name = self._get_table_name(alias, catalog)
        schema, name = table_name.split(".")
        return self._get_partition_values(schema, name)
    
    def _get_table_columns(self, catalog_name: str, table_name: str) -> List[str]:
        """
        Busca as colunas de uma tabela

        :param catalog_name (str): O nome do catálogo
        :param table_name (str): O nome da tabela
        :return: A lista de colunas de partição
        """
        return [
            r[0]
            for r in self.spark.sql(f"DESCRIBE TABLE {catalog_name}.{table_name}")
            .select("col_name")
            .collect()
        ]
    
    def get_table_columns(self, alias: str, catalog: str = "infer") -> List[str]:
        """
        Busca as colunas de uma tabela

        :param alias (str): Alias usado no registro da tabela no arquivo de bases
        :param catalog (str): nome do catalog que retem a base de dados (infer, external, internal)
        :return: A lista de colunas de partição
        """
        table_name = self._get_table_name(alias, catalog)
        schema, name = table_name.split(".")
        return self._get_table_columns(schema, name)
    
    def _get_table_metadata(self, catalog_name: str, table_name: str) -> Dict[str, str]:
        """
        Busca os metadados de uma tabela

        :param catalog_name (str): O nome do catálogo
        :param table_name (str): O nome da tabela
        :return: A lista de colunas de partição
        """
        try:
            desc = self.spark.sql(f"DESCRIBE TABLE EXTENDED {catalog_name}.{table_name}")
            return {
                "Name": desc.filter(F.col("col_name") == "Table").collect()[0][1],
                "DatabaseName": desc.filter(F.col("col_name") == "Database").collect()[0][1],
                "Owner": desc.filter(F.col("col_name") == "Owner").collect()[0][1],
                "CreateTime": desc.filter(F.col("col_name") == "Created Time").collect()[0][1],
                "Location": desc.filter(F.col("col_name") == "Location").collect()[0][1],
                "InputFormat": desc.filter(F.col("col_name") == "InputFormat").collect()[0][1],
                "OutputFormat": desc.filter(F.col("col_name") == "OutputFormat").collect()[0][1],
                "Statistics": desc.filter(F.col("col_name") == "Statistics").collect()[0][1],
                "CreatedBy": desc.filter(F.col("col_name") == "Created By").collect()[0][1],
            }
        except Exception as e:
            print(f"Error retrieving table metadata: {e}")
            return dict()

    def get_table_metadata(self, alias: str, catalog: str = "infer") -> List[str]:
        """
        Busca os metadados de uma tabela

        :param alias (str): Alias usado no registro da tabela no arquivo de bases
        :param catalog (str): nome do catalog que retem a base de dados (infer, external, internal)
        :return: A lista de colunas de partição
        """
        table_name = self._get_table_name(alias, catalog)
        schema, name = table_name.split(".")
        return self._get_table_metadata(schema, name)

    def _get_environment_table_location(self, table_name: str) -> str:
        """
        Obtem o caminho no bucket s3 onde uma table está contida dentro do ambiente
        
        :param table_name (str): O nome da tabela a ser lida
        :return: string com o caminho no s3
        """
        for c in ["internal", "external"]:
            if self._prefixos[c] != "":
                if table_name.startswith(self._prefixos[c]):
                    table_name = table_name.replace(self._prefixos[c], "")
        location = f"{self.bucket}/{table_name}"
        return location
    
    def _get_table_location(self, source_catalog: str, table_name: str) -> str:
        """
        Obtem o caminho no bucket s3 onde uma table está contida
        
        :param table_name (str): O nome da tabela a ser lida
        :param source_catalog (str): O nome do catálogo de origem
        :return: string com o caminho no s3
        """
        try:
            response = self.spark.sql(f"DESCRIBE FORMATTED {source_catalog}.{table_name}")
            return response.filter(F.col("col_name") == "Location").select("data_type").head()[0]
        except Exception as e:
            print(f"Error retrieving table location: {e}")
            return f"{self.bucket}/{table_name}"
    
    def _read_data(
        self,
        table_name: str,
        source_catalog: Optional[str] = None,
        partition_values: Optional[Dict] = None
    ) -> DataFrame:
        """
        Função utilizada para ler uma tabela do Glue

        :param table_name (str): O nome da tabela a ser lida
        :param source_catalog (Optional(str)): O nome do catálogo de origem
        :param partition_values (Optional[Dict]): Os valores das partições a serem usadas
        :return: O DataFrame
        """
        if not source_catalog:
            source_catalog = self._schema
        
        df = self.spark.table(f"{source_catalog}.{table_name}")

        if partition_values:
            partition_filters = [f"{key} = '{value}'" for key, value in partition_values.items()]
            combined_filter = " AND ".join(partition_filters)
            df = df.filter(combined_filter)

        return df

    def get_by_name(
        self, alias: str, catalog: str = "infer", partition_values: Optional[Dict] = None
    ) -> DataFrame:
        """
        Função utilizada para ler uma tabela do Glue

        :param alias (str): Alias usado no registro da tabela no arquivo de bases
        :param catalog (str): nome do catalog que retem a base de dados (infer, external, internal)
        :param partition_values (Optional[Dict]): Os valores das partições a serem usadas
        :return: O DataFrame
        """
        table_name = self._get_table_name(alias, catalog)
        schema, name = table_name.split(".")
        return self._read_data(table_name=name, source_catalog=schema, partition_values=partition_values)
    
    def get_unregistered(
        self, table_name: str, partition_values: Optional[Dict] = None
    ) -> DataFrame:
        """
        Carrega uma tabela que não está catalogada pela ferramenta, mas está salva no ambiente
        instanciado

        :param table_name (str): nome da tabela a ser carregada
        :param partition_values (Optional[Dict]): Os valores das partições a serem usadas
        :return: O DataFrame
        """
        return self._read_data(table_name=table_name, source_catalog=self._schema, partition_values=partition_values)
    
    def _write_data(
        self, 
        df: DataFrame,
        table_name: str,
        partitions: Optional[List[str]] = None, 
        options: Optional[Dict] = None, 
        mode: Optional[str] = "append",
        unregistered: bool = False,
    ) -> None:
        """
        Função utilizada para escrever um DataFrame em uma tabela do Glue

        :param df (DataFrame): O DataFrame a ser escrito
        :param table_name (str): O nome da tabela a ser escrita (schema.nome)
        :param partitions (Optional[List[str]]): As partições a serem usadas
        :param options (Optional[Dict]): As opções a serem usadas
        :param mode (Optional[str]): Modo de escrita, se sobreposicao ou concatenacao
        :param unregistered (bool): Flag se devemos ignorar o prefixo
        """
        schema, table = table_name.split(".")

        if schema != self._schema:
            raise ValueError(f"Você não pode escrever para outros schemas -> {schema}")
        
        if unregistered:
            path_s3 = f"{self.bucket}/{table}"
        else:
            path_s3 = self._get_environment_table_location(table)
        
        if path_s3 in list(BUCKETS.values()) + [
            self.bucket, f"{self.bucket}/", "/".join(self.bucket.split("/")[:-1])
        ]:
            raise ValueError(f"Você está escrevendo para uma pasta não permitida - {path_s3}")
        
        writer = df.write.format("parquet")
        if partitions:
            writer = writer.partitionBy(partitions)
        if options:
            writer = writer.options(**options)
        catalog, _ = table_name.split(".")
        self.spark.sql(f"USE {catalog}")

        writer.saveAsTable(table_name, mode=mode, path=path_s3)

        self.spark.sql(f"MSCK REPAIR TABLE {table_name};")

    def write_by_name(
        self, 
        df: DataFrame,
        alias: str, 
        catalog: str = "infer",
        partitions: Optional[List[str]] = None, 
        options: Optional[Dict] = None, 
        mode: Optional[str] = "append",
    ) -> None:
        """
        Função utilizada para escrever um DataFrame em uma tabela do Glue

        :param df (DataFrame): O DataFrame a ser escrito
        :param alias (str): Alias / apelido dado a tabela no registro do arquivo de bases
        :param catalog (str): nome do catalog que retem a base de dados (infer, external, internal)
        :param partitions (Optional[List[str]]): As partições a serem usadas
        :param options (Optional[Dict]): As opções a serem usadas
        :param mode (Optional[str]): Modo de escrita, se sobreposicao ou concatenacao
        """
        table_name = self._get_table_name(alias, catalog)
        schema, _ = table_name.split(".")
        assert schema == self._schema, "Você só pode escreve para os ambientes registrados"
        self._write_data(
            df=df,
            table_name=table_name,
            partitions=partitions, 
            options=options, 
            mode=mode,
        )

    def write_unregistered(
        self, 
        df: DataFrame,
        table_name: str,
        partitions: Optional[List[str]] = None, 
        options: Optional[Dict] = None, 
        mode: Optional[str] = "append",
    ) -> None:
        """
        Função utilizada para escrever um DataFrame em uma tabela do Glue

        :param df (DataFrame): O DataFrame a ser escrito
        :param table_name (str): O nome da tabela a ser escrita
        :param partitions (Optional[List[str]]): As partições a serem usadas
        :param options (Optional[Dict]): As opções a serem usadas
        :param mode (Optional[str]): Modo de escrita, se sobreposicao ou concatenacao
        """
        for k, tn in self.internal_catalog.items():
            _, table = tn.split(".")
            if table_name == self._prefixos["internal"] + table:
                raise ValueError(
                    f"Não é possível escrever sem registro para uma tabela catalogada -> {table}=({k}, internal)"
                )
        for k, tn in self.external_catalog.items():
            schema, table = tn.split(".")
            if schema == self._schema:
                if table_name == self._prefixos["external"] + table:
                    raise ValueError(
                        f"Não é possível escrever sem registro para uma tabela catalogada -> {table_name}=({k}, external)"
                    )

        self._write_data(
            df=df,
            table_name=f"{self._schema}.{table_name}",
            partitions=partitions, 
            options=options, 
            mode=mode,
            unregistered=True,
        )

    def copy_data_from(
        self, 
        alias: str, 
        catalog: str = "infer", 
        source_env: Optional["DataUtils"] = None, 
        env_name: Optional[str] = None, 
        **init_params
    ) -> None:
        """
        Função utilizada para copiar dados de uma tabela para outra

        :param alias (str): O alias da tabela a ser copiada
        :param catalog (str): nome do catalog que retem a base de dados (infer, external, internal)
        :param source_env(DataUtils): O ambiente de onde copiar a tabela
        :param env_name (str): O nome do ambiente de onde copiar a tabela
        :param init_params: Parâmetros de inicialização do data utils
        """
        if source_env is None:
            source_env = self.__class__(env_name, **init_params)
        df = source_env.get_by_name(alias, catalog)
        partitions = source_env.get_partition_columns(alias, catalog)
        self.write_by_name(
            df=df,
            alias=alias, 
            catalog=catalog,
            partitions=partitions, 
            options=dict(overwriteSchema=True), 
            mode="overwrite",
        )
    
    def copy_environment(
        self, 
        source_env: Optional["DataUtils"] = None, 
        env_name: Optional[str] = None, 
        **init_params
    ) -> None:
        """
        Função utilizada para copiar dados de uma tabela para outra

        :param source_env(DataUtils): O ambiente de onde copiar a tabela
        :param env_name (str): O nome do ambiente de onde copiar a tabela
        :param init_params: Parâmetros de inicialização do data utils
        """
        if source_env is None:
            source_env = self.__class__(env_name, **init_params)
        tabelas = source_env.list_available_tables()
        for catalog, table in tabelas:
            if catalog == "internal":
                self.copy_data_from(alias=table, catalog=catalog, source_env=source_env)
