if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    data = data[(data['passenger_count'] > 0) & (data['trip_distance'] > 0)]
    data['lpep_pickup_date'] = data['lpep_pickup_datetime'].dt.date
    data.columns =  data.columns.str.replace(r'([a-z])([A-Z])', r'\1_\2').str.lower()

    return data


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.

    """
    assert (output['passenger_count'] > 0).all()
    assert (output['trip_distance'] > 0).all()
    assert (output['vendor_id'].notnull()).all()
    assert output is not None, 'The output is undefined'
