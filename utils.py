import functools
from oslo_log import log as logging

LOG = logging.getLogger(__name__)

def pure_driver_debug_trace(f):
    """Log the method entrance and exit including active backend name.

    This should only be used on VolumeDriver class methods. It depends on
    having a 'self' argument that is a PureBaseVolumeDriver.
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        driver = args[0]  # self
        cls_name = driver.__class__.__name__
        method_name = "%(cls_name)s.%(method)s" % {"cls_name": cls_name,
                                                   "method": f.__name__}
        backend_name = driver._get_current_array(True).backend_id
        LOG.debug("[%(backend_name)s] Enter %(method_name)s, args=%(args)s,"
                  " kwargs=%(kwargs)s",
                  {
                      "method_name": method_name,
                      "backend_name": backend_name,
                      "args": args,
                      "kwargs": kwargs,
                  })
        result = f(*args, **kwargs)
        LOG.debug("[%(backend_name)s] Leave %(method_name)s, ret=%(result)s",
                  {
                      "method_name": method_name,
                      "backend_name": backend_name,
                      "result": result,
                  })
        return result

    return wrapper
