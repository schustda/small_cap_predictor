from model.training_data import TrainingData



def test_getIdxs():
    td = TrainingData()
    idxs = td.get_idxs('working_train')
    assert type(idxs) == list

def test_modelDevelopment():
    td = TrainingData()
    td.model_development_split()
