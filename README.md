# Bootcamp: Engenheiro(a) de Dados Cloud

## Desafio Prático

### Módulo 1: Fundamentos em Arquitetura de Dados e Soluções em Nuvem

## Objetivos:
> Arquiteturas de Dados em Nuvem; <br>
> Implementação de Data Lake em solução Cloud de Storage; <br>
> Implementação de Processamento de Big Data; <br>
> Esteiras de Deploy, utilizando o Github; <br>
> IaC com Terraform.


## Arquitetura
![image](https://user-images.githubusercontent.com/28718418/215929021-d72b810c-683b-439b-b3ab-eef86fec61bb.png)


### Provider de preferência:
- A Amazon Web Services (AWS) é a plataforma de nuvem mais adotada e mais abrangente do mundo, oferecendo mais de 200 serviços completos de datacenters em todo o mundo.

### Atividades
1. Realizar a ingestão dos dados de VÍNCULOS PÚBLICOS da RAIS 2020 no AWS S3. 
    - Dados disponíveis em: [microdados rais e caged](http://pdet.mte.gov.br/microdados-rais-e-caged) 

2. Realizar tratamento no dataset da RAIS 2020  <br>
    a. Modifique os nomes das colunas, trocando espaços por “_”; <br>
    b. Retire acentos das colunas e coloque todas as letras minúsculas; <br>
    c. Crie uma coluna “uf” através da coluna "municipio"; <br>
    d. Realize os ajustes no tipo de dado para as colunas de remuneração.

3. Transformar os dados no formato parquet e escrevê-los na zona staging ou zona silver do seu Data Lake.

4. Fazer a integração com alguma engine de Data Lake. No caso da AWS, você deve:
    a. Configurar um Crawler para a pasta onde os arquivos na staging estão depositados;
    b. Validar a disponibilização no Athena.

5. Caso deseje utilizar o Google, disponibilize os dados para consulta usando o Big Query. Caso utilize outra nuvem, a escolha da engine de Data Lake é livre.

6. Use a ferramenta de Big Data ou a engine de Data Lake (ou o BigQuery, se escolher trabalhar com Google Cloud) para investigar os dados e responder às perguntas do desafio.

7. Quando o desenho da arquitetura estiver pronto, crie um repositório no Github (ou Gitlab, ou Bitbucket, ou outro de sua escolha) e coloque o código IaC para a implantação da infraestrutura. Nenhum recurso deve ser implantado manualmente.
