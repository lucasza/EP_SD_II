package br.com.mapreduce;

public class Constants {
    //Commands
    static final String COMMAND_LEAST_SQUARE = "least";
    static final String COMMAND_MEAN = "mean";
    static final String COMMAND_STD_DEVIATION = "stddev";
    //Commands Explanation
    static final String COMMAND_EXPLANATION_LEAST_SQUARE = "aplica o método dos mínimos quadrados em um conjunto de dados e inteiro x. O conjunto de dados é limitado por uma medição <measure> que assume um dos campos do arquivo de entrada como TEMP, WSPD e etc; por um período de tempo definido por <start date (yyyyMMdd)> <end date (yyyyMMdd)>, que pode assumir por exemplo 19300201, que siginifica 01 de fevereiro de 1930; e por uma estação de medição que é um inteiro. A inteiro x é o ponto em que se quer realizar a projeção dos dados que será o retorno do job.";
    static final String COMMAND_EXPLANATION_MEAN = "aplica a média do conjunto de dados. O conjunto de dados é limitado por uma medição <measure> que assume um dos campos do arquivo de entrada como TEMP, WSPD e etc; por um período de tempo definido por <start date (yyyyMMdd)> <end date (yyyyMMdd)>, que pode assumir por exemplo 19300201, que siginifica 01 de fevereiro de 1930; e por uma estação de medição que é um inteiro.";
    static final String COMMAND_EXPLANATION_STD_DEVIATION = "aplica o desvio padrão do conjunto de dados. O conjunto de dados é limitado por uma medição <measure> que assume um dos campos do arquivo de entrada como TEMP, WSPD e etc; por um período de tempo definido por <start date (yyyyMMdd)> <end date (yyyyMMdd)>, que pode assumir por exemplo 19300201, que siginifica 01 de fevereiro de 1930; e por uma estação de medição que é um inteiro.";

    //Commands arguments
    public static final String COMMAND_ARGUMENTS_LEAST_SQUARE = 	"least <input path> <output path> <work station number> <start date (yyyyMMdd)> <end date (yyyyMMdd)> <measure> <x>";
    public static final String COMMAND_ARGUMENTS_STATION_GREP = 	"station <input path> <output path> <work station number>";
    public static final String COMMAND_ARGUMENTS_DATE_GREP = 		"date <input path> <output path> <start date (yyyyMMdd)> <end date (yyyyMMdd)>";
    public static final String COMMAND_ARGUMENTS_MEAN = 			"mean <input path> <output path> <work station number> <start date (yyyyMMdd)> <end date (yyyyMMdd)> <measure>";
    public static final String COMMAND_ARGUMENTS_STD_DEVIATION = 	"stddev <input path> <output path> <work station number> <start date (yyyyMMdd)> <end date (yyyyMMdd)> <measure>";

    //FIELDs on the Set of Weather Data
    public static final String[] FIELDS = {
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
