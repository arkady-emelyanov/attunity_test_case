from transform.lib.args import get_args
from transform.lib.spark import get_spark

from transform.changes import apply_changes_task
from transform.load import initial_load_task
from transform.snapshot import snapshot_task
from transform.vacuum import vacuum_task

mappings = {
    "initial_load_task": initial_load_task,
    "apply_changes_task": apply_changes_task,
    "snapshot_task": snapshot_task,
    "vacuum_task": vacuum_task,
}

cmd_args = get_args()
task = mappings.get(cmd_args.task)
if not callable(task):
    raise Exception(f"Non-callable task: '{cmd_args.task}'")

spark = get_spark()
task(spark, cmd_args)
