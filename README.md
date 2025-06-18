# T&D Formatos de Tabela

## Objetivo

Comparar o desempenho de escrita e o tamanho final dos arquivos gerados ao converter dados de arquivos JSON para os formatos **Delta Lake** e **Apache Hudi** (utilizando o modo *Merge On Read* com base **ORC**).

## Dados de Entrada

* **Kindle\_Store\_5.json** e **Movies\_and\_TV\_5.json**
* Fonte: conjuntos de dados da Amazon disponíveis em [https://nijianmo.github.io/amazon/index.html](https://nijianmo.github.io/amazon/index.html).

## Metodologia

1. **Pré-processamento** (notebook `preprocessing.ipynb`):

   * Importação dos dois arquivos JSON em DataFrames PySpark.
   * Alinhamento de esquema e concatenação dos DataFrames para gerar um JSON unificado de base para o experimento.
2. **Salvamento no formato Delta** (notebook `saving_delta.ipynb`):

   * Leitura do JSON unificado.
   * Escrita no formato Delta Lake em modo `overwrite`.
3. **Salvamento no formato Hudi (Merge On Read – ORC)** (notebook `saving_hudi.ipynb`):

   * Leitura do JSON unificado.
   * Escrita no formato Hudi com opção de operação `bulk_insert`, `recordkey.field=id` e tabela `my_hudi_table`, em modo `overwrite` e base **ORC**.
4. **Análise de resultados** (notebook `analysis.ipynb`):

   * Coleta das métricas de tempo de escrita e tamanho em disco.
   * Comparação dos resultados apresentados.

## Resultados Obtidos

| Experimento    | Tamanho (MB) | Tempo (s) |
| -------------- | -----------: | --------: |
| **delta\_1**   |     1819.304 |    23.488 |
| **hudi\_1**    |       76.067 |    36.396 |
| **united\_df** |     3917.894 |    32.000 |

## Análise dos Resultados

* **Delta Lake (`delta_1`)**:

  * **Vantagem**: menor tempo de escrita (≈23,5 s).
  * **Desvantagem**: maior consumo de espaço em disco (≈1.819 MB).

* **Apache Hudi (Merge On Read, ORC) (`hudi_1`)**:

  * **Vantagem**: redução significativa de espaço em disco (≈76 MB).
  * **Desvantagem**: maior tempo de escrita (≈36,4 s).

* **United\_df** (benchmark combinado):

  * Tempo intermediário (32,0 s) e maior tamanho (≈3.918 MB), refletindo o total bruto antes de compactação/OTimizações.

## Conclusões e Recomendações

* Para **ingestão rápida**, prefira **Delta Lake**.
* Para **otimização de armazenamento**, utilize **Apache Hudi** em modo *Merge On Read* (ORC).
* Ajuste a escolha conforme o trade-off entre latência de escrita e footprint de disco.

## Reprodutibilidade

1. Clone este repositório e acesse a pasta de notebooks.
2. Abra e execute os notebooks na ordem:

   * `preprocessing.ipynb`
   * `saving_delta.ipynb`
   * `saving_hudi.ipynb`
   * `analysis.ipynb`
3. Garanta as mesmas versões de **PySpark**, **Delta Lake** e **Hudi** conforme especificado nos notebooks.
4. Ajuste caminhos de entrada e saída, se necessário, e colete métricas de tempo e tamanho.
