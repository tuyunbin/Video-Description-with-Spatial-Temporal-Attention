from jobman import api0, sql
from jobman.tools import DD, flatten

# Experiment function
from mlp_jobman import experiment

# Database
TABLE_NAME = 'mlp_dumi'
db = api0.open_db('postgres://ift6266h10@gershwin/ift6266h10_sandbox_db?table='+TABLE_NAME)

# Default values
state = DD()
state.learning_rate = 0.01
state.L1_reg = 0.00
state.L2_reg = 0.0001
state.n_iter = 50
state.batch_size = 20
state.n_hidden = 10

# Hyperparameter exploration
for n_hidden in 20, 30:
    state.n_hidden = n_hidden

    # Explore L1 regularization w/o L2
    state.L2_reg = 0.
    for L1_reg in 0., 1e-6, 1e-5, 1e-4:
        state.L1_reg = L1_reg

        # Insert job
        sql.insert_job(experiment, flatten(state), db)

    # Explore L2 regularization w/o L1
    state.L1_reg = 0.
    for L2_reg in 1e-5, 1e-4:
        state.L1_reg = L1_reg

        # Insert job
        sql.insert_job(experiment, flatten(state), db)

# Create the view
db.createView(TABLE_NAME+'_view')
