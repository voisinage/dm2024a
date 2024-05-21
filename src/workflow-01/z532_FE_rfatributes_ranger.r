# Agregado de atributos derivados del Random Forest

# limpio la memoria
rm(list = ls(all.names = TRUE)) # remove all objects
gc(full = TRUE) # garbage collection

require("data.table")
require("yaml")

require("ranger")
require("randomForest") # solo se usa para imputar nulos

#cargo la libreria
# args <- c( "~/dm2024a" )
args <- commandArgs(trailingOnly=TRUE)
source( paste0( args[1] , "/src/lib/action_lib.r" ) )

#------------------------------------------------------------------------------
# agrega al dataset nuevas variables {0,1}
#  que provienen de las hojas de un Random Forest

AgregaVarRandomForest <- function(
    num.trees, max.depth,
    min.node.size, mtry, semilla) {

  cat( "inicio AgregaVarRandomForest()\n")
  gc()

  dataset[, clase01 := 0L ]
  dataset[ get(envg$PARAM$dataset_metadata$clase) %in% envg$PARAM$train$clase01_valor1, 
      clase01 := 1L ]

  campos_buenos <- setdiff(colnames(dataset), envg$PARAM$dataset_metadata$clase )

  dataset_rf <- copy(dataset[, campos_buenos, with = FALSE])
  set.seed(semilla, kind = "L'Ecuyer-CMRG")
  azar <- runif(nrow(dataset_rf))

  dataset_rf[, entrenamiento :=
    as.integer( get(envg$PARAM$dataset_metadata$periodo) %in% envg$PARAM$train$training &
      (clase01 == 1 | azar < envg$PARAM$train$undersampling))]

  # Controla que todas las variables sean numéricas
  # Convertir las columnas no numéricas ni de factor a tipo numérico o de factor
  columnas_numericas <- sapply(dataset_rf, is.numeric)
  columnas_factor <- sapply(dataset_rf, is.factor)

  # Iterar sobre las columnas y convertir las que no son numéricas ni de factor

  for (i in 1:ncol(dataset_rf)) {
    if (!columnas_numericas[i] && !columnas_factor[i]) {
      dataset_rf[[i]] <- as.numeric(dataset_rf[[i]])
      # O bien, si es adecuado, convertir a factor:
      # dataset_rf[[i]] <- as.factor(dataset_rf[[i]])
    }
    }

  # imputo los nulos, ya que ranger no acepta nulos
  # Leo Breiman, ¿por que le temias a los nulos?
  set.seed(semilla, kind = "L'Ecuyer-CMRG")
  dataset_rf <- na.roughfix(dataset_rf)

  campos_buenos <- setdiff(
    colnames(dataset_rf),
    c(envg$PARAM$dataset_metadata$clase, "entrenamiento")
  )

  set.seed(semilla, kind = "L'Ecuyer-CMRG")
  modelo <- ranger(
    formula = "clase01 ~ .",
    data = dataset_rf[entrenamiento == 1L, campos_buenos, with = FALSE],
    classification = TRUE,
    probability = FALSE,
    num.trees = num.trees,
    max.depth = max.depth,
    min.node.size = min.node.size,
    mtry = mtry,
    seed = semilla
    # num.threads = 1
  )

  rfhojas <- predict(
    object = modelo,
    data = dataset_rf[, campos_buenos, with = FALSE],
    predict.all = TRUE, # entrega la prediccion de cada arbol
    type = "terminalNodes" # entrega el numero de NODO el arbol
  )

  for (arbol in 1:num.trees) {
    hojas_arbol <- unique(rfhojas$predictions[, arbol])

    for (pos in 1:length(hojas_arbol)) {
      # el numero de nodo de la hoja, estan salteados
      nodo_id <- hojas_arbol[pos]
      dataset[, paste0(
        "rf_", sprintf("%03d", arbol),
        "_", sprintf("%03d", nodo_id)
      ) := 0L]

      dataset[
        which(rfhojas$predictions[, arbol] == nodo_id, ),
        paste0(
          "rf_", sprintf("%03d", arbol),
          "_", sprintf("%03d", nodo_id)
        ) := 1L
      ]
    }
  }

  rm(dataset_rf)
  dataset[, clase01 := NULL]

  gc()
  cat( "fin AgregaVarRandomForest()\n")
}
#------------------------------------------------------------------------------
#------------------------------------------------------------------------------
# Aqui empieza el programa
cat( "z532_FE_rfatributes_ranger.r  START\n")
action_inicializar() 

envg$PARAM$RandomForest$semilla <- envg$PARAM$semilla
  
# cargo el dataset donde voy a entrenar
# esta en la carpeta del exp_input y siempre se llama  dataset.csv.gz
# cargo el dataset
envg$PARAM$dataset <- paste0( "./", envg$PARAM$input, "/dataset.csv.gz" )
envg$PARAM$dataset_metadata <- read_yaml( paste0( "./", envg$PARAM$input, "/dataset_metadata.yml" ) )

cat( "lectura del dataset\n")
action_verificar_archivo( envg$PARAM$dataset )
cat( "Iniciando lectura del dataset\n" )
dataset <- fread(envg$PARAM$dataset)
cat( "Finalizada lectura del dataset\n" )


colnames(dataset)[which(!(sapply(dataset, typeof) %in% c("integer", "double")))]


GrabarOutput()

#--------------------------------------
# estas son las columnas a las que se puede agregar
#  lags o media moviles ( todas menos las obvias )

campitos <- c( envg$PARAM$dataset_metadata$primarykey,
  envg$PARAM$dataset_metadata$entity_id,
  envg$PARAM$dataset_metadata$periodo,
  envg$PARAM$dataset_metadata$clase )

campitos <- unique( campitos )

cols_lagueables <- copy(setdiff(
  colnames(dataset),
  campitos
))

# ordeno el dataset por primary key
#  es MUY  importante esta linea
# ordeno dataset
cat( "ordanado dataset\n")
setorderv(dataset, envg$PARAM$dataset_metadata$primarykey)


#------------------------------------------------------------------------------
# Agrego variables a partir de las hojas de un Random Forest

envg$OUTPUT$AgregaVarRandomForest$ncol_antes <- ncol(dataset)

AgregaVarRandomForest(
  num.trees = envg$PARAM$RandomForest$num.trees,
  max.depth = envg$PARAM$RandomForest$max.depth,
  min.node.size = envg$PARAM$RandomForest$min.node.size,
  mtry = envg$PARAM$RandomForest$mtry,
  semilla = envg$PARAM$RandomForest$semilla
)

envg$OUTPUT$AgregaVarRandomForest$ncol_despues <- ncol(dataset)
GrabarOutput()
gc()



# grabo el dataset
cat( "grabado dataset\n")
fwrite(dataset,
  file = "dataset.csv.gz",
  logical01 = TRUE,
  sep = ","
)

# copia la metadata sin modificar
cat( "grabado metadata\n")
write_yaml( envg$PARAM$dataset_metadata, 
  file="dataset_metadata.yml" )

#------------------------------------------------------------------------------

# guardo los campos que tiene el dataset
tb_campos <- as.data.table(list(
  "pos" = 1:ncol(dataset),
  "campo" = names(sapply(dataset, class)),
  "tipo" = sapply(dataset, class),
  "nulos" = sapply(dataset, function(x) {
    sum(is.na(x))
  }),
  "ceros" = sapply(dataset, function(x) {
    sum(x == 0, na.rm = TRUE)
  })
))

fwrite(tb_campos,
  file = "dataset.campos.txt",
  sep = "\t"
)

#------------------------------------------------------------------------------
cat( "Fin del programa\n")

envg$OUTPUT$dataset$ncol <- ncol(dataset)
envg$OUTPUT$dataset$nrow <- nrow(dataset)

envg$OUTPUT$time$end <- format(Sys.time(), "%Y%m%d %H%M%S")
GrabarOutput()
#------------------------------------------------------------------------------
# finalizo la corrida
#  archivos tiene a los files que debo verificar existen para no abortar

action_finalizar( archivos = c("dataset.csv.gz","dataset_metadata.yml")) 
cat( "z532_FE_rfatributes_ranger.r  END\n")
