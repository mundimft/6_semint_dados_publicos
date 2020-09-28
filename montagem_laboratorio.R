library(RCurl)

library(vroom)
library(data.table)
library(RSQLite)
library(DBI)
library(tictoc)
library(fs)
library(pryr)
library(stringi)
library(stringr)
library(dplyr)
library(arkdb)
library(tm)

#library(googledrive) # verrificar se baixar o arquivo direto do google melhora a situação



#* Download -  
#- Baixando 1 arquivo  
#- Baixando multiplos arquivos  
#- usando `for` para baixar muitos arquivos 

## Dados de dividas
#diretorio para amazenamento
dir.create("./divida")

#PGFN Divida Ativa Geral
download.file("http://dadosabertos.pgfn.gov.br/Dados_abertos_Nao_Previdenciario.zip", "./divida/Dados_abertos_Nao_Previdenciario.zip", method = "auto")

#Divida FGTS
download.file("http://dadosabertos.pgfn.gov.br/Dados_abertos_FGTS.zip", "./divida/Dados_abertos_FGTS.zip", method = "auto")

#Divida Previdenciaria

download.file("http://dadosabertos.pgfn.gov.br/Dados_abertos_Previdenciario.zip", "./divida/Dados_abertos_Previdenciario.zip", method = "auto")

#tamanho da base 1Gb


#Dados do CAGED
#diretorio para armazenamento
dir.create("./caged")

#FTP de acesso para os microdados
#atencao o mês foi especificiado manualmente, até a confeção deste os dados do mês de agosto não foram disponibilizados
url = "ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/Movimenta%E7%F5es/2020/Julho/"
filenames = getURL(url, ftp.use.epsv = FALSE, dirlistonly = TRUE)
filenames <- strsplit(filenames, "\r\n")
filenames = unlist(filenames)

filenames

#laço baixando todos os arquivos dentro da pasta 
for (filename in filenames) {
  download.file(paste(url, filename, sep = ""), paste(getwd(), "/caged/", filename,
                                                      sep = ""), method = "curl")
}

#cuidado com o metodo ao baixar do FTP o metodo pode corromper o arquivo  

#todos os cnpjs e cnaes e cnaes Secundários
dir.create("./cnpj")

#maior base de dados 2.5Gb o tempo pode variar bem confome o a conexão
download.file("https://data.brasil.io/dataset/socios-brasil/empresa.csv.gz", "./cnpj/empresa.csv.gz", method = "auto")
download.file("https://data.brasil.io/dataset/socios-brasil/cnae_secundaria.csv.gz", "./cnpj/cnae_secundaria.csv.gz", method = "auto")
download.file("https://data.brasil.io/dataset/socios-brasil/cnae_cnpj.csv.gz", "./cnpj/cnae_cnpj.csv.gz", method = "auto")

#3Gb de download

#aqui propor uma abordagem paralela,A base é grande e demora xx minutos para ser ingerida para um banco caso queira poderá ser baixada montada em um banco
#agora 
  
#tamanho de variavel 
  
#* Leitura (usar microbench para avaliar os métodos de entrada)  
#-  
#  - Lendo 1 arquivo (data.table, vroom)
#-object.size() - ver o tamanho dos itens em memória
#- Lendo multiplos arquivos (cuidado com o tamanho do arquivo!!) 
#- usando `for` para ler multiplos arquivos

##Caso os tenha caracteres com encoding latin é necessário passar ao vroom para que não sejam imputados quebrados nos DF
locale_padrao_pt <- locale("pt", encoding = "latin1")

#primeira pegadinha, o VROOM pode ser muito rápido, porem se você tiver multiplos arquivos dentro do zip ele precisa de uma ajudinha

dados_fgts_com_vroom <- vroom("./divida/Dados_abertos_FGTS.zip", locale = locale_padrao_pt ) # aqui temos 1170 Observacoes 

tic()
dados_fgts_com_data_table <- data.table::fread("unzip -cq ./divida/Dados_abertos_FGTS.zip") # aqui temos 440915 observacoes
toc()

#Criando um wrapper para ler todos os dados dentro do arquivo para o VROOM ler de forma correta
ler_arquivos_dentro_zip <- function(file, ...) {
  nome_dos_arquivos <- unzip(file, list = TRUE)$Name
  vroom(purrr::map(nome_dos_arquivos, ~ unz(file, .x)), locale = locale_padrao_pt,   ...)
}

#lembrara de remover pontos e tracos do cnpj fara a realizacao das consultas no sqlite
tic()
leitura_direta_vroom <- ler_arquivos_dentro_zip("./divida/Dados_abertos_FGTS.zip")
toc()

tic()
leitura_direta_fread <- data.table::fread("unzip -cq ./divida/Dados_abertos_FGTS.zip", encoding = "Latin-1")
toc()

#Qunato temos de dados baixados
#o diretório de trabalho neste momento com as bases de dados baixadas está com 
tamanho_diretorio <- (sum(file.info(list.files(".", all.files = TRUE, recursive = TRUE))$size))/1024^3
#6.4Gb compactados só a base de CNPJ´s que tem aproximadamente 2.5Gb compactada ocupa 8Gb aproximadamente descompactada se fizemos a leitura dela no R direto para uma variável 
# você não terá memória para carregar outros dados e realizar manipulações ...

#vamos descompactar 1 banco e colocar na memoria?
#colcaremos a base do caged na memoria, vamos ver se cabe, e ela tem uma peculiaridade, foi compactada em 7z e a tabulaçao é ";" o que deixa as coisas um menos redondas para alguns
#pacotes como para ingerir em banco ou memoria

#descompacte todos os arquivos baixados da base na pasta caged
tic()
caged_lista_arquivos <- fs::dir_ls("./caged/", glob = "*txt")
caged_movimentacao <- vroom(caged_lista_arquivos, col_types = list(.default = "c", saldomovimentação = "i" ))
toc() # aproximadamente 5 segundo para gerar agrupar os dados 

mem_used()
#carregando a lista de movimentação do caged deste ano (jan a jul) consumimos  321Mb de memória 




#Porem temos que ler todos e passar para o banco

removePunctuation(dados_fgts_com_vroom$CPF_CNPJ) -> dados_fgts_com_vroom$CPF_CNPJ


###
#* A memória pode encher e agora ?
#  -  
#  - SQL (a sitaxe) - igual companheiro  `selecione e conte as coisas do móvel que estão abertas e agrupe por tipo`!! :)
#- BDI - API para consulta em bancos
#- RSQLite - dados em disco não incomodam a memória


#- Explicando o Banco
#aqui os dados serão apresentados 
#- Visualizando os dados (DB Explorer)
#- Visualizando os dados do banco (pragma do banco, tabela etc)
#- Visalizando os dados no banco (evitar full-scan) `LIMIT X`


##Como criar este banco, como vamos agrupar todos os dados. 

#crinado o seu primeiro banco de dados


 con <- dbConnect(RSQLite::SQLite(), "meu_primeiro_banco.db")
 
 
 
 #lembrar sempre de disconectar do banco
 dbDisconnect(con)

#- Fazendo a ingestão dos dados - 1 data frame;
 
#como já carregamos os dados na memória de algumas bases vamos passar estes dados para o banco sql

 #porem antes de passar estes dados para o banco vamos verificar os nomes das colunas para nao psssar caracteres acentuados pois dificultam as queryes
caged_movimentacao %>%
 names() %>%
   stri_trans_general("Latin-ASCII") %>%
   str_replace_all(c(" " = "_")) %>%
   toupper() -> names(caged_movimentacao) 


 
 tic()
#fazendo a ingestão da tabela de CNAE´s
dbWriteTable(con,
            "db_caged_movimentao",
            caged_movimentacao)
toc()
#231 segundos para a ingestão dos dados com tipo Chr custa tempo passar tudo como texto. será necessário analisar os tipos dos campos
# 45 segundos para ingestão destes dados


#Importamos um data.frame, porem temos arquivos que estão zipados que gostaíamos de importar sem enviar para a memóra e este é o desfio 


dbDisconnect(con)



#* Salvando a query em uma nova tabela (para usar como subquery por exemplo);
#* Salvando a consulta em um data.frame (usar em subquery ou realiza análise no R);
#- Fazendo a ingestão dos dados 1 arquivo
#- Criar a tabela de itens a serem filtrados
#- Fazendo a ingestão dos dados (multiplos arquivos);
  # Criar index e testar a performance
#- Jogar na memória ;
#- Imputar os arquivos `APPEND`;
#- Passando pela memória (oneroso) sei fazer;
#- Fazendo a ingestão direta, estou no caminho;
#- Me deram conexão para um banco com muitas linhas e muitos recursos (o_O)(mostrar no Oracle) 
#- Bonus Track - DuckDB (banco otimizado para uso analítico) - Não sei se vou conseguir implantar a tempo. 
