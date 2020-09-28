#teste de coltipe com vroom


teste_vroom <- vroom("./divida/Dados_abertos_FGTS.zip", locale = locale_padrao_pt, col_types = list(VALOR_CONSOLIDADO = "d" ) )

colnames(tes)



library(arkdb)


files <- fs::dir_ls("./cnpj/")


#esta funcao e necessaria para alterar o tratamento de entrada dos dados, se não foi feito desta maneira os dados entram em formado ruim
tabela_temp_streaming_cnpj <- function() {
  streamable_table(
    function(file, ...) readr::read_csv(file,col_types = list(.default = "c"),  ...),
    function(x, path, omit_header)
      readr::write_tsv(x = x, path = path, omit_header = omit_header),
    "tsv")
}

#importando a tabela de cnae por empresa
tic()
unark("./cnpj/cnae_secundaria.csv.gz", con, lines=100000, streamable_table = tabela_temp_streaming_cnpj() )
toc()

#509 segundos - 8 minutos 
#257 segundos - 4 minutos para a tabela cnae_secandária 1000000 chuncks
#304 segundos - 5 minutos para a tabela cane_secundaria 10000000 chuncks
#267 segundos  - 4 minutos para tabela cnae_secundaria 100000 chunks


#importanto a tabela com todos os cnpjs

tic()
unark("./cnpj/empresa.csv.gz", con, lines=100000, streamable_table = tabela_temp_streaming_cnpj() )
toc()
#3041 segundos, estamos falando de 50 minutos para ler todo o banco com todas as empresas e todos os campos, para o andamento da oficina 
#sera necessario escolhers as colunas para que os dados sejam carregados mais rápido 

pegar_query <- dbGetQuery(con, "select * from cnae_secundaria limit 10")


as.data.frame(pegar_query)


dbDisconnect(con)

#criando tabela com base em query

"CREATE TABLE suppliers
AS (SELECT *
    FROM companies
    WHERE id > 1000);"

""


select subclasse, sum(saldomovimentacao) from movimentacao_caged
where subclasse in (8230001, 8230002)
group by subclasse

select cnae_fiscal, count(*) from empresa
where cnae_fiscal in (8230001, 9319101, 5620102, 7220004, 8230002)
group by cnae_fiscal

select  cnae_fiscal,sum(valor_consolidado), count(*) from divida_fgts
left join lista_cnpj on divida_fgts.CPF_CNPJ = lista_cnpj.cnpj
where CPF_CNPJ in (select cnpj from lista_cnpj)
group by cnae_fiscal

select  cnae_fiscal,count(*) from divida_fgts
left join lista_cnpj on divida_fgts.CPF_CNPJ = lista_cnpj.cnpj
where CPF_CNPJ in (select cnpj from lista_cnpj)
