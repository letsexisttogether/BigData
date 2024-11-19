from pyspark.ml.feature import Tokenizer, StopWordsRemover, CountVectorizer

from general import get_df


def execute(spark):
    print("Executing task #4")

    df = get_df(spark)

    df = tokenize(df)

    stop_words = [str(i) for i in range(0, 10)]
    df = remove_stop_words(df, stop_words)
    count_words(df)


def tokenize(df):
    tokenizer = Tokenizer(
        inputCol='gamename',
        outputCol='tokens'
    )

    tokenized_df = tokenizer.transform(df)

    print('Tokenized DataFrame:')
    tokenized_df.show()

    return tokenized_df


def remove_stop_words(df, stop_words):
    remover = StopWordsRemover(
        inputCol='tokens',
        outputCol='filtered_tokens'
    )

    remover.setStopWords(stop_words)

    filtered_df = remover.transform(df)

    print('Filtered DataFrame:')
    filtered_df.show()

    return filtered_df


def count_words(df):
    counter = CountVectorizer(
        inputCol='filtered_tokens',
        outputCol='words_count'
    )

    counted_df = counter.fit(df).transform(df)

    print('DataFrame with words count:')
    counted_df.show()
