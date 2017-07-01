package br.com.mapreduce;

public class Constants {
    static final String MINIMO_QUADRADO = "least";
    static final String COMMAND_MEAN = "mean";
    static final String DESVIO_PADRAO = "stddev";
    static final String EXPLICACAO_MINIMO_QUADRADO = "aplica o método dos mínimos quadrados em um conjunto de dados e inteiro x. O conjunto de dados é limitado por uma medição <measure> que assume um dos campos do arquivo de entrada como TEMP, WSPD e etc; por um período de tempo definido por <start date (yyyyMMdd)> <end date (yyyyMMdd)>, que pode assumir por exemplo 19300201, que siginifica 01 de fevereiro de 1930; e por uma estação de medição que é um inteiro. A inteiro x é o ponto em que se quer realizar a projeção dos dados que será o retorno do job.";
    static final String COMMAND_EXPLANATION_MEAN = "aplica a média do conjunto de dados. O conjunto de dados é limitado por uma medição <measure> que assume um dos campos do arquivo de entrada como TEMP, WSPD e etc; por um período de tempo definido por <start date (yyyyMMdd)> <end date (yyyyMMdd)>, que pode assumir por exemplo 19300201, que siginifica 01 de fevereiro de 1930; e por uma estação de medição que é um inteiro.";
    static final String EXPLICACAO_DESVIO_PADRAO = "aplica o desvio padrão do conjunto de dados. O conjunto de dados é limitado por uma medição <measure> que assume um dos campos do arquivo de entrada como TEMP, WSPD e etc; por um período de tempo definido por <start date (yyyyMMdd)> <end date (yyyyMMdd)>, que pode assumir por exemplo 19300201, que siginifica 01 de fevereiro de 1930; e por uma estação de medição que é um inteiro.";
    public static final String MINIMO_QUADRADO_ARGS = "least <input path> <output path> <work station number> <start date (yyyyMMdd)> <end date (yyyyMMdd)> <measure> <x>";
    public static final String GREP_ESTACAO_ARGS = "station <input path> <output path> <work station number>";
    public static final String GREP_DATA_ARGS = "date <input path> <output path> <start date (yyyyMMdd)> <end date (yyyyMMdd)>";
    public static final String COMMAND_ARGUMENTS_MEAN = "mean <input path> <output path> <work station number> <start date (yyyyMMdd)> <end date (yyyyMMdd)> <measure>";
    public static final String DESVIO_PADRAO_ARGS = "stddev <input path> <output path> <work station number> <start date (yyyyMMdd)> <end date (yyyyMMdd)> <measure>";

    public static final String[] COLUNAS = {
        "STN---",
        "WBAN",
        "YEARMODA",
        "TEMP",
        "DEWP",
        "SLP",
        "STP",
        "VISIB",
        "WDSP",
        "MXSPD",
        "GUST",
        "MAX",
        "MIN",
        "PRCP",
        "SNDP",
        "FRSHTTYEARMODA",
        "TEMP",
        "DEWP",
        "SLP",
        "STP",
        "VISIB",
        "WDSP",
        "MXSPD",
        "GUST",
        "MAX",
        "MIN",
        "PRCP",
        "SNDP",
        "FRSHTT"
    };

}
