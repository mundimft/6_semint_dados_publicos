library(RCurl)
library(bench)
library(sqldf)
library(vroom)
library(data.table)
library(microbenchmark)



#* Download -  
#- Baixando 1 arquivo  
#- Baixando multiplos arquivos  
#- usando `for` para baixar muitos arquivos 

#PGFN Divida Ativa Geral
download.file("http://dadosabertos.pgfn.gov.br/Dados_abertos_Nao_Previdenciario.zip", "\\divida", method = "libcurl")

#Divida FGTS
download.file("http://dadosabertos.pgfn.gov.br/Dados_abertos_Nao_Previdenciario.zip", "\\divida", method = "libcurl")

#Divida Previdenciaria

download.file("http://dadosabertos.pgfn.gov.br/Dados_abertos_Previdenciario.zip
              ", "\\divida", method = "libcurl")

#caged
url = "ftp://ftp.mtps.gov.br/pdet/microdados/NOVO%20CAGED/Movimenta%E7%F5es/2020/Julho/"
filenames = getURL(url, ftp.use.epsv = FALSE, dirlistonly = TRUE)
filenames <- strsplit(filenames, "\r\n")
filenames = unlist(filenames)


#laço baixando arquivos
for (filename in filenames) {
  download.file(paste(url, filename, sep = ""), paste(getwd(), "/caged/", filename,
                                                      sep = ""))
}



#* Leitura (usar microbench para avaliar os métodos de entrada)  
#-  
#  - Lendo 1 arquivo (data.table, vroom)
#-object.size() - ver o tamanho dos itens em memória
#- Lendo multiplos arquivos (cuidado com o tamanho do arquivo!!) 
#- usando `for` para ler multiplos arquivos




#primeira pegadinha, o VROOM pode ser muito rápido, porem se você tiver multiplos arquivos dentro do zip ele não lê

a <- vroom("./divida/Dados_abertos_FGTS.zip") # aqui temos 1170 Observacoes 
b <- data.table::fread("unzip -cq ./divida/Dados_abertos_FGTS.zip") # aqui temos 440915 observacoes

#Criando um wrapper para ler todos os dados dentro do arquivo
read_all_zip <- function(file, ...) {
  filenames <- unzip(file, list = TRUE)$Name
  vroom(purrr::map(filenames, ~ unz(file, .x)), ...)
}


read_all_zip("./divida/Dados_abertos_FGTS.zip")
data.table::fread("unzip -cq ./divida/Dados_abertos_FGTS.zip")





#* A memória enxeu e agora ?
#  -  
#  - SQL (a sitaxe) - igual companheiro  `selecione e conte as coisas do móvel que estão abertas e agrupe por tipo`!! :)
#- BDI - API para consulta em bancos
#- RSQLite - dados parados em disco não incomodam a memória
#- Explicando o Banco
#- Visualizando os dados (DB Explorer)
#- Visualizando os dados do banco (pragma do banco, tabela etc)
#- Visalizando os dados no banco (evitar full-scan) `LIMIT X`
#- Fazendo a ingestão dos dados - 1 data frame;
#* Salvando a query em uma nova tabela (para usar como subquery por exemplo);
#* Salvando a consulta em um data.frame (usar em subquery ou realiza análise no R);
#- Fazendo a ingestão dos dados 1 arquivo
#- Criar a tabela de itens a serem filtrados
#- Fazendo a ingestão dos dados (multiplos arquivos);
#- Jogar na memória ;
#- Imputar os arquivos `APPEND`;
#- Passando pela memória (oneroso) sei fazer;
#- Fazendo a ingestão direta, estou no caminho;
#- Me deram conexão para um banco com muitas linhas e muitos recursos (o_O)(mostrar no Oracle) 
#- Bonus Track - DuckDB (banco otimizado para uso analítico) - Não sei se vou conseguir implantar a tempo. 
