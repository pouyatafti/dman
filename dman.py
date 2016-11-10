from functools import reduce

class DMan:
    def __init__(self, pingfun, createfun, quiet=True):
        self._registered = {}
        self._pingfun = pingfun        # pingfun(key)
        self._createfun = createfun    # createfun(key)
        self._quiet = quiet

    def _print(*args, **kwargs):
        if not self._quiet:
            print(*args, **kwargs)
        
    def isregistered(self, key):
        return key in self._registered

    def ping(self, key):
        if key not in self._registered:
            raise ValueError("[%s] unkown key" % str(key))
        return self._pingfun(key)
    
    def getdeps(self, key):
        if key not in self._registered:
            raise ValueError("[%s] unknown key" % str(key))
        return self._registered[key]
    
    def setdeps(self, key, deps, add=False):
        if add:
            if key in self._registered:
                raise ValueError("[%s] key already registered; use add=False to modify existing entry" % str(key))
        else:
            if key not in self._registered:
                raise ValueError("[%s] unknown key; use add=True to register new entry" % str(key))
    
        if all(dep in self._registered for dep in deps):
            self._registered[key] = deps
        else:
            raise ValueError("[%s] entry has unknown dependencies" % str(key))
            
    def register(self, ds_dict):
        valid, err = self.validate(ds_dict)

        if not valid:
            raise ValueError(err)

        self._registered = ds_dict

    def validate(self, ds_dict):
        all_keys = set(ds_dict.keys())
        all_deps = reduce(lambda d1,d2: set(d1) | set(d2),ds_dict.values(),set())
        if not all_deps <= all_keys:
            return (False, "unknown dependency")

        for key in all_keys:
            if not self._validate_recursive(ds_dict, key, [], []):
                return (False, "circular dependency")

        return (True, "")

    def _validate_recursive(self, ds_dict, key, resolved, unresolved):
        unresolved.append(key)
        deps = ds_dict[key]
        for dep in deps:
            if dep not in resolved:
                if dep in unresolved:
                    return False
                rv = self._validate_recursive(ds_dict, dep, resolved, unresolved)

        resolved.append(key)
        unresolved.remove(key)
        return rv
        
    def create(self, key):
        if self.ping(key):
            self._print("[%s] already exists" % str(key))
        else:
            self._create_recursive(key,[],[])
            
    def _create_recursive(self, key, resolved, unresolved, prefix=""):
        self._print("%s>>> [%s] creating dependencies..." % (prefix,str(key)))
        unresolved.append(key)
        deps = self._registered[key]
        for dep in deps:
            if dep not in resolved:
                if dep in unresolved:
                    raise ValueError("[%s -&gt; %s] circular dependency" % (str(key), str(dep)))
                self._create_recursive(dep,resolved,unresolved,"==" + prefix)
        
        if not self._create1(key,"==" + prefix):
            raise Exception("[%s] creation failed" % str(key))
        else:
            self._print("%s<<< [%s] done" % (prefix,str(key)))

        resolved.append(key)
        unresolved.remove(key)
        return resolved
    
    def _create1(self,key,prefix=""):
        if not self._pingfun(key):
            self._print("%s ... [%s] creating..." % (prefix,str(key)))
            self._createfun(key)
        else:
            self._print("%s ... [%s] already exists" % (prefix,str(key)))
        
        return self._pingfun(key)
