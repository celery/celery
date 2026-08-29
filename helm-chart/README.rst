Helm Chart for Celery
=====================

This helm chart can be used for deploying Celery in local or a kubernetes server.

It contains following main folders/files:

::

    helm-chart
    ├── Chart.yaml
    ├── README.rst
    ├── templates
    │   ├── _helpers.tpl
    │   ├── configmap.yaml
    │   ├── deployment.yaml
    │   ├── secret.yaml
    │   └── serviceaccount.yaml
    └── values.yaml

The most important file here will be ``values.yaml``.
This will be used for setting/altering parameters, most of the parameters are annotated inside ``values.yaml`` with comments.

Deploying on Cluster:
--------------------

If you want to setup and test on local, check out: `setting up on local`_

To install on kubernetes cluster run following command from root of project:

::

    helm install celery helm-chart/

You can also setup environment-wise value files, for example: ``values_dev.yaml`` for ``dev`` env,
then you can use following command to override the current ``values.yaml`` file's parameters to be environment specific:

::

    helm install celery helm-chart/ --values helm-chart/values_dev.yaml

To upgrade an existing installation of chart you can use:

::

    helm upgrade --install celery helm-chart/

    or

    helm upgrade --install celery helm-chart/ --values helm-chart/values_dev.yaml


You can uninstall the chart using helm:

::

    helm uninstall celery

.. _setting up on local:

Setting up on local:
--------------------
To setup kubernetes cluster on local use the following link:

- k3d_
- `Colima (recommended if you are on MacOS)`_

.. _`k3d`: https://k3d.io/v5.7.3/
.. _`Colima (recommended if you are on MacOS)`: https://github.com/abiosoft/colima?tab=readme-ov-file#kubernetes

You will also need following tools:

- `helm cli`_
- `kubectl`_

.. _helm cli: https://helm.sh/docs/intro/install/
.. _kubectl: https://kubernetes.io/docs/tasks/tools/
