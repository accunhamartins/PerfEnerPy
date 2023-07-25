import pandas as pd
artifact_0 = pd.read_parquet('artifacts/artifact_0.parquet')
artifact_1 = pd.read_parquet('artifacts/artifact_1.parquet')
artifact_2 = pd.read_parquet('artifacts/artifact_2.parquet')
artifact_3 = artifact_2.sample(frac=0.37)
artifact_4 = artifact_3[['CYayh__word']]
artifact_5 = artifact_4.sample(frac=0.39)
artifact_6 = artifact_5.sample(frac=0.82)
artifact_7 = artifact_6.sample(frac=0.82)
artifact_8 = artifact_7.sample(frac=0.79)
artifact_9 = artifact_8.sample(frac=0.55)
