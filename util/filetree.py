import shutil
from pathlib import Path
from shutil import rmtree
from typing import Optional, Union

import enoslib as en

TAG_IDENT = "@"
VAR_IDENT = "$"

NONE_TAG = "__none__"


class MissingPathException(Exception):
    pass


def _resolve_paths(path: str, args: Optional[list[Union[list, dict]]] = None):
    if args is None:
        resolved_paths = [path]
    else:
        resolved_paths = []

        for arg in args:
            segments = []

            for segment in path.split("/"):
                if segment.startswith(VAR_IDENT):
                    key = segment[len(VAR_IDENT):]
                    if isinstance(arg, list):
                        segments.append(arg[int(key)])
                    elif isinstance(arg, dict):
                        segments.append(arg[key])
                else:
                    segments.append(segment)

            resolved_paths.append("/".join(segments))

    return resolved_paths


def _resolve_tags(tags: Optional[list[str]] = None):
    if tags is None:
        resolved_tags = [NONE_TAG]
    else:
        resolved_tags = tags

    return resolved_tags


def _tree(spec: list[dict]) -> dict[str, list[Path]]:
    tags = {}

    for spec_entry in spec:
        resolved_paths = []

        for path in _resolve_paths(path=spec_entry["path"], args=spec_entry["args"] if "args" in spec_entry else None):
            segments = path.split("/")

            seg_0 = segments[0]
            seg_rest = segments[1:]

            if seg_0.startswith(TAG_IDENT):
                var = seg_0[len(TAG_IDENT):]
                if var in tags:
                    path = "/".join(seg_rest)
                    for basepath in tags[var]:
                        resolved_paths.append(basepath / path)
            else:
                resolved_paths.append(Path(path))

        for tag in _resolve_tags(tags=spec_entry["tags"] if "tags" in spec_entry else None):
            if tag not in tags:
                tags[tag] = []

            tags[tag].extend(resolved_paths)

    return tags


class FileTree:
    def __init__(self):
        self.tree = {}

    def define(self, spec):
        for spec_entry in spec:
            resolved_paths = []

            for path in _resolve_paths(path=spec_entry["path"],
                                       args=spec_entry["args"] if "args" in spec_entry else None):
                segments = path.split("/")

                seg_0 = segments[0]
                seg_rest = segments[1:]

                if seg_0.startswith(TAG_IDENT):
                    var = seg_0[len(TAG_IDENT):]
                    if var in self.tree:
                        path = "/".join(seg_rest)
                        for basepath in self.tree[var]:
                            resolved_paths.append(basepath / path)
                else:
                    resolved_paths.append(Path(path))

            for tag in _resolve_tags(tags=spec_entry["tags"] if "tags" in spec_entry else None):
                if tag not in self.tree:
                    self.tree[tag] = []

                self.tree[tag].extend(resolved_paths)

        return self

    def build(self, remote: Optional[list[en.Host]] = None):
        if remote is None:
            for path in self.iterpaths():
                path.mkdir(mode=0o777, parents=True, exist_ok=True)
        else:
            with en.actions(roles=remote) as actions:
                for path in self.iterpaths():
                    if path.is_dir():
                        actions.file(path=str(path), state="directory", mode=0o777)

        return self

    def copy(self, file_paths: list[Path], tag: str, remote: Optional[list[en.Host]] = None):
        if remote is None:
            for path in self.iterpaths(tag):
                for file_path in file_paths:
                    shutil.copy2(file_path, path)
        else:
            with en.actions(roles=remote) as actions:
                for path in self.iterpaths(tag):
                    for file_path in file_paths:
                        actions.copy(src=str(file_path), dest=str(path))

        return self

    def remove(self, tag: str, remote: Optional[list[en.Host]] = None):
        if remote is None:
            for path in self.iterpaths(tag):
                rmtree(path)
        else:
            with en.actions(roles=remote) as actions:
                for path in self.iterpaths(tag):
                    actions.file(path=str(path), state="absent")

        return self

    def iterpaths(self, tag: Optional[str] = None):
        if tag is None:
            for _tag in self.tree:
                for path in self.tree[_tag]:
                    yield path
        else:
            for path in self.tree[tag]:
                yield path

    def paths(self, tag: str):
        if tag in self.tree:
            return self.tree[tag]

        return []

    def path(self, tag: str):
        if tag in self.tree:
            return self.tree[tag][0]

        raise MissingPathException
