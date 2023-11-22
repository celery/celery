import pytest

from celery import shared_task


@shared_task()
def dummy_task(x, y):
    return x + y


class test_loader:
    def test_autodiscovery__when_packages_exist(self, manager):
        # Arrange
        expected_package_name, _, module_name = __name__.rpartition('.')
        unexpected_package_name = 'datetime.datetime'

        # Act
        manager.app.autodiscover_tasks([expected_package_name, unexpected_package_name], module_name, force=True)

        # Assert
        assert f'{expected_package_name}.{module_name}.dummy_task' in manager.app.tasks
        assert not any(
            task.startswith(unexpected_package_name) for task in manager.app.tasks
        ), 'Expected datetime.datetime to neither have test_loader module nor define a Celery task.'

    def test_autodiscovery__when_packages_do_not_exist(self, manager):
        # Arrange
        existent_package_name, _, module_name = __name__.rpartition('.')
        nonexistent_package_name = 'nonexistent.package.name'

        # Act
        with pytest.raises(ModuleNotFoundError) as exc:
            manager.app.autodiscover_tasks(
                [existent_package_name, nonexistent_package_name], module_name, force=True
            )

        # Assert
        assert nonexistent_package_name.startswith(exc.value.name), 'Expected to fail on importing "nonexistent"'
