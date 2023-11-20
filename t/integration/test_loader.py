from celery import shared_task


@shared_task()
def dummy_task(x, y):
    return x + y


class test_loader:
    def test_autodiscovery(self, manager):
        # Arrange
        expected_package_name, _, module_name = __name__.rpartition('.')
        unexpected_package_name = 'nonexistent.package.name'

        # Act
        manager.app.autodiscover_tasks([expected_package_name, unexpected_package_name], module_name, force=True)

        # Assert
        assert f'{expected_package_name}.{module_name}.dummy_task' in manager.app.tasks
        assert not any(
            task.startswith(unexpected_package_name) for task in manager.app.tasks
        )
