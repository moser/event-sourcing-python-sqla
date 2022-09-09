import pytest
import uuid
from nextversion.domain import Task


def test_it():
    task = Task.create(
        title="Do it", description="Long text of what to do.", project_id=uuid.uuid4()
    )
    assert task.title == "Do it"

    task.update_title(title="Do it later")
    assert task.title == "Do it later"

    with pytest.raises(TypeError):
        task.update_title(title=123)
