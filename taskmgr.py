import argparse
import os
import json
from typing import Union, Optional, List, Any, AnyStr
import re
import parse
import time

__VERSION__ = 'v0.0.1'


def get_argparser():
    parser = argparse.ArgumentParser('training task manager - %s' % __VERSION__)
    parser.add_argument('-d', '--duration', type=int, default=7,
                        help='show logs within a duration (in days), default: 7 days')

    subparsers = parser.add_subparsers(help='subcommand help', dest='command')

    op_add_parser = subparsers.add_parser('add', help='add watching group')
    op_add_parser.add_argument('tag', type=str, help='watching group tag')
    op_add_parser.add_argument(
        'path', nargs='*', type=str, help='watching paths to be added to this group (dirs or files)')
    op_add_parser.add_argument('-p', '--pattern', nargs='+', type=str, default=[],
                               help='patterns to be added to this group')

    op_list_parser = subparsers.add_parser('list', help='list watching group info')
    op_list_parser.add_argument('tag', nargs='*', type=str, help='groups to be shown')
    op_list_parser.add_argument('-P', '--path', action='store_true', help='list watching paths')
    op_list_parser.add_argument('-p', '--pattern', action='store_true',
                                help='list watching patterns')

    op_rm_parser = subparsers.add_parser('rm', help='rm watching groups')
    op_rm_parser.add_argument(
        'tag', nargs='+', type=str, help='groups to be removed (will deactivate these groups if activated, rm a deactivated group will permanently delete it)')

    op_del_parser = subparsers.add_parser(
        'delete', help='delete paths and patterns in a watching group')
    op_del_parser.add_argument('tag', type=str, help='watching group tag')
    op_del_parser.add_argument(
        'path', nargs='*', type=str, help='watching paths to be deleted from this group (dirs or files)')
    op_del_parser.add_argument('-p', '--pattern', nargs='+', type=str, default=[],
                               help='patterns to be deleted from this group')

    op_rename_parser = subparsers.add_parser(
        'rename', help='rename watching group tag')
    op_rename_parser.add_argument('oldtag', type=str, help='watching group tag')
    op_rename_parser.add_argument('newtag', type=str, help='watching group new tag')

    op_arrange_parser = subparsers.add_parser(
        'arrange', help='arrange the order of patterns in a watching group, please use the index in `list -p`')
    op_arrange_parser.add_argument('tag', type=str, help='watching group tag')
    op_arrange_parser.add_argument('indices', nargs='*', type=int)

    op_config_parser = subparsers.add_parser(
        'config', help='config group settings')
    op_config_parser.add_argument('tag', type=str, help='watching group tag')
    op_config_parser.add_argument('-p', '--pattern', nargs=2, action='append')
    op_config_parser.add_argument('-a', '--delete-paths', dest='allpath', action='store_true')
    op_config_parser.add_argument('-A', '--delete-patterns', dest='allcfg', action='store_true')
    mgroup = op_config_parser.add_mutually_exclusive_group()
    mgroup.add_argument('-d', '--disable', dest='default', action='store_const', const=False,
                        help='disable default patterns')
    mgroup.add_argument('-e', '--enable', dest='default', action='store_const', const=True,
                        help='enable default patterns')
    op_config_parser.add_argument(
        '-f', '--file', type=str, help='use pattern {#name} to recognize item name, e.g., {name}/log.txt. default: {name}.{ext}')
    op_config_parser.add_argument(
        '-E', '--end', type=str, help='ending indicating task is done, default "done" (case insensitive)')
    op_config_parser.add_argument(
        '-x', '--exclude', nargs='+', help='exclude names')
    op_config_parser.add_argument(
        '-i', '--include', nargs='+', help='include names')

    op_status_parser = subparsers.add_parser('status', help='show training status')
    op_status_parser.add_argument('tag', nargs='*', help='groups to show')
    op_status_parser.add_argument('-d', '--duration', type=int, default=7,
                                  help='show logs within a duration (in days), default: 7 days')

    op_inspect_parser = subparsers.add_parser('inspect', help='inspect details')
    op_inspect_parser.add_argument('tag', type=str, help='groups to show')
    op_inspect_parser.add_argument('name', type=str,
                                   help='show details given the name')

    return parser


class TaskManager(object):

    @staticmethod
    def _ensure_sequential(data: Any):
        if not isinstance(data, (list, tuple)):
            return (data, )
        else:
            return data

    @staticmethod
    def init_config():
        return {
            'version': __VERSION__,
            'activated_groups': {},
            'deactivated_groups': {},
        }

    @staticmethod
    def default_group():
        return {
            'fnp': '{name}.{ext}',
            'ending': 'done',
            'builtin_func': True,
            'paths': [],
            'patterns': [],
            'excluded': [],
            'included': [],
        }

    @staticmethod
    def save_cfg(path: AnyStr, data: Any):
        with open(path, 'w') as fd:
            json.dump(data, fd, sort_keys=True, indent=2)

    @staticmethod
    def load_cfg(path: AnyStr, create_if_not_exists: bool = True):
        if not os.path.exists(path):
            if create_if_not_exists:
                TaskManager.save_cfg(path, TaskManager.init_config())
            else:
                raise FileNotFoundError(path)
        with open(path) as fd:
            data = json.load(fd)
        ret = TaskManager.init_config()
        ret.update(data)
        for group_type in ['activated_groups', 'deactivated_groups']:
            for one in ret[group_type]:
                tmp = TaskManager.default_group()
                tmp.update(ret[group_type][one])
                ret[group_type][one] = tmp
        return ret

    def __init__(self, cfg_path: AnyStr = '~/.tmgr.conf', create_if_not_exists: bool = True, width: int = 80):
        self.cfg_path = os.path.abspath(os.path.expanduser(cfg_path))
        self.cfg = self.load_cfg(self.cfg_path, create_if_not_exists=create_if_not_exists)
        self.width = width

    def save(self):
        self.save_cfg(self.cfg_path, self.cfg)

    def __getitem__(self, tag: AnyStr):
        if tag not in self.cfg['activated_groups']:
            raise KeyError(tag)
        return self.cfg['activated_groups'][tag]

    def config(self, tag: AnyStr, fnp: Optional[AnyStr] = None, ending: Optional[AnyStr] = None, builtin_func: Optional[bool] = None,
               remove_all_path: Optional[bool] = None, remove_all_patterns: Optional[bool] = None, include: Optional[List[AnyStr]] = None, exclude: Optional[List[AnyStr]] = None):
        item = self[tag]
        if fnp is not None:
            item['fnp'] = str(fnp)
        if ending is not None:
            item['ending'] = str(ending)
        if builtin_func is not None:
            item['builtin_func'] = bool(builtin_func)
        if remove_all_path:
            item['paths'] = []
        if remove_all_patterns:
            item['patterns'] = []
        if exclude is not None:
            item['excluded'].extend(exclude)
            item['excluded'] = list(set(item['excluded']))
            item['included'] = list(set(item['included']) - set(item['excluded']))
        if include is not None:
            item['included'].extend(include)
            item['included'] = list(set(item['included']))
            item['excluded'] = list(set(item['excluded']) - set(item['included']))
        self.save()

    def activate(self, tags: Union[AnyStr, List[AnyStr]]):
        tags = self._ensure_sequential(tags)
        for tag in tags:
            if tag in self.cfg['deactivated_groups']:
                if tag not in self.cfg['activated_groups']:
                    v = self.cfg['deactivated_groups'][tag]
                    self.cfg['activated_groups'][tag] = v
                del self.cfg['deactivated_groups'][tag]
            if tag not in self.cfg['activated_groups']:
                self.cfg['activated_groups'][tag] = self.default_group()
        self.save()

    def deactivate(self, tags: Union[AnyStr, List[AnyStr]]):
        tags = self._ensure_sequential(tags)
        for tag in tags:
            if tag in self.cfg['activated_groups']:
                v = self.cfg['activated_groups'][tag]
                self.cfg['deactivated_groups'][tag] = v
                del self.cfg['activated_groups'][tag]
            elif tag in self.cfg['deactivated_groups']:
                del self.cfg['deactivated_groups'][tag]
        self.save()

    def add_paths(self, tag: AnyStr, paths: Union[AnyStr, List[AnyStr]]):
        item = self[tag]
        paths = list(map(lambda x: os.path.abspath(x), self._ensure_sequential(paths)))
        item['paths'].extend(paths)
        self.save()

    def del_paths(self, tag: AnyStr, paths: Union[AnyStr, List[AnyStr]]):
        item = self[tag]
        paths = set(map(lambda x: os.path.abspath(x),  self._ensure_sequential(paths)))
        new_paths = []
        for path in item['paths']:
            abspath = os.path.abspath(path)
            if abspath not in paths:
                new_paths.append(path)
        item['paths'] = new_paths
        self.save()

    def add_patterns(self, tag: AnyStr, patterns: Union[AnyStr, List[AnyStr]]):
        item = self[tag]
        patterns = self._ensure_sequential(patterns)
        item['patterns'].extend(patterns)
        self.save()

    def del_patterns(self, tag: AnyStr, patterns: Union[AnyStr, List[AnyStr]]):
        item = self[tag]
        patterns = self._ensure_sequential(patterns)
        rm_set = set()
        p_set = set()
        for pattern in patterns:
            if isinstance(pattern, str) and pattern.isdigit():
                rm_set.add(int(pattern))
            else:
                p_set.add(pattern)
        new_patterns = []
        for i, pattern in enumerate(item['patterns']):
            if i not in rm_set and pattern not in p_set:
                new_patterns.append(pattern)
        item['patterns'] = new_patterns
        self.save()

    def arrange(self, tag: AnyStr, indices: List[int]):
        item = self[tag]
        n = len(indices)
        p = set(indices)
        if len(p) != n:
            raise IndexError(str(indices))
        for i in range(n):
            if i not in p:
                raise IndexError(str(indices))
        new_patterns = [item['patterns'][int(i)] for i in indices]
        item['patterns'] = new_patterns
        self.save()

    def edit_patterns(self, tag: AnyStr, index: Union[str, int], p: AnyStr):
        item = self[tag]
        if isinstance(index, str):
            if not index.isdigit():
                raise IndexError(index)
            else:
                index = int(index)
        elif not isinstance(index, int):
            raise IndexError(str(index))
        index = int(index)
        if not (index >= 0 and index < len(item['patterns'])):
            raise IndexError(str(index))
        item['patterns'][index] = p
        self.save()

    def mv(self, oldtag: AnyStr, newtag: AnyStr):
        if newtag in self.cfg['activated_groups']:
            raise ValueError(newtag)
        if oldtag not in self.cfg['activated_groups']:
            raise KeyError(oldtag)
        self.cfg['activated_groups'][newtag] = self.cfg['activated_groups'][oldtag]
        del self.cfg['activated_groups'][oldtag]
        self.save()

    def group_config_view(self, group_type: AnyStr, tags: List[AnyStr], showPath: bool = False, showPattern: bool = False):
        t_set = set(tags)
        ret_str = []
        for tag in sorted(self.cfg[group_type].keys()):
            if not t_set or tag in t_set:
                item = self.cfg[group_type][tag]
                tmp_str = '\033[1;35m[{}]\033[0m `{}`: {} paths {} patterns'.format(
                    tag, item['fnp'], len(item['paths']), len(item['patterns']))
                if item['builtin_func']:
                    tmp_str += '*'
                ret_str.append(tmp_str)
                if showPath:
                    ret_str.append('  paths:')
                    for p in item['paths']:
                        p = os.path.relpath(p, os.getcwd())
                        ret_str.append('    \033[1;34m'+p+'\033[0m')
                if showPattern:
                    ret_str.append('  patterns:')
                    for i, p in enumerate(item['patterns']):
                        ret_str.append('   \033[1;32m[{}]\033[0m \033[1;34m{}\033[0m'.format(i, p))
        return ret_str

    def render_config_view(self, tags: List[AnyStr], **kwargs):
        ret_str = []
        a_groups = self.group_config_view(
            'activated_groups', tags, **kwargs)
        if a_groups:
            ret_str.append('activated groups:')
            for line in a_groups:
                ret_str.append('  ' + line)
        d_groups = self.group_config_view(
            'deactivated_groups', tags, **kwargs)
        if d_groups:
            ret_str.append('deactivated groups:')
            for line in d_groups:
                ret_str.append('  ' + line)
        if not a_groups and not d_groups:
            ret_str.append('Not any groups')
        return '\n'.join(ret_str)

    @staticmethod
    def match_file(fnp: AnyStr, fn: AnyStr):
        # fnp_re = re.sub(r'\{#name\}', r'[^/\\\]+', fnp) + '$'
        # fnp_re = re.sub(r'\{#digit\}', r'[\\d]+', fnp_re)
        # fnp_re = re.sub(r'\{#char\}', r'[\\w]+', fnp_re)
        # fnp_re = re.sub(r'\{#mix\}', r'[\\w\\d]+', fnp_re)
        # fnp_re = re.sub(r'\{#any\}', r'[^/\\\]+', fnp_re)
        # fnp_re = re.sub(r'\.', r'\.', fnp_re)
        # c = re.compile(fnp_re)
        # f = c.findall(fn)
        fn_reversed = fn[::-1]
        fnp_reversed = fnp[::-1].replace('{', '!|*#').replace('}', '{').replace('!|*#', '}')
        res = parse.parse(fnp_reversed, fn_reversed)
        if res:
            return True, res['eman'][::-1]
        else:
            return False, None

    def group_panel_view(self, tag: AnyStr, width: Optional[int] = None, duration=None, inspect: Optional[AnyStr] = None):
        recs = {}
        item = self[tag]
        logs = {}
        for path in item['paths']:
            for dn, _, fns in os.walk(path):
                for fn in fns:
                    fullfn = os.path.join(dn, fn)
                    ok, matched_name = self.match_file(item['fnp'], fullfn)
                    if ok:
                        logs.setdefault(matched_name, []).append(fullfn)
        include_set = set(item['included'])
        exclude_set = set(item['excluded'])
        for one in sorted(logs.keys()):
            fns = sorted(logs[one], key=lambda x: os.path.getmtime(x), reverse=True)
            stamp = os.path.getmtime(fns[0])
            delta = int(time.time() - stamp)
            if inspect is None and (isinstance(duration, (int, float)) and delta > duration and one not in include_set or one in exclude_set) \
                    or inspect is not None and one != inspect:
                continue
            patterns = list(item['patterns'])
            if item['builtin_func']:
                patterns.insert(
                    0, r'eta[^\d\w]*(?:[\d\.:]+|[ ,]|days|d|hrs|hours|hour|d|mins|minutes|secs|sec|seconds)+')
                patterns.insert(0, r'loss[^\d]*[\d\.]+')
                patterns.insert(0, r'iter[^\d]*[\d]+')
                patterns.insert(0, r'epoch[^\d]*[\d]+')
            patterns.insert(0, item['ending'])  # ending detection must be the first pattern
            found = {}
            ptr = -1
            remain = len(patterns)
            while remain > 0 and ptr + 1 < len(fns):
                ptr += 1
                fn = fns[ptr]
                with open(fn) as fd:
                    text = fd.read()
                tmp = {}
                for i, pattern in enumerate(patterns):
                    if pattern not in found:
                        c = re.compile(pattern, re.IGNORECASE)
                        results = c.findall(text)
                        if results:
                            if i == 0:
                                tmp[pattern] = ptr
                            else:
                                tmp[pattern] = results[-1]
                            remain -= 1
                found.update(tmp)
            flag = None
            item_str = []
            for i, pattern in enumerate(patterns):
                if i == 0:
                    if pattern in found and found[pattern] == 0:
                        flag = 1
                    else:
                        flag = 0
                else:
                    if pattern in found:
                        if not item_str:
                            item_str.append(' ' + found[pattern])
                        else:
                            if width is None or len(item_str[-1]) + len(found[pattern]) + 3 <= width:
                                item_str[-1] += ' | ' + found[pattern]
                            else:
                                item_str.append(' ' + found[pattern])
            for tp, line in enumerate(item_str):
                if tp & 1:
                    item_str[tp] = '\033[1;37m' + line
                item_str[tp] += '\033[0m'
            if inspect is not None:
                item_str.append(' ' + '---' * 3)
                item_str.extend(map(lambda x: ' * ' + x, fns))
            d = delta // 86400
            h = (delta - d * 86400) // 3600
            m = (delta - d * 86400 - h * 3600) // 60
            s = (delta - d * 86400 - h * 3600 - m * 60) // 1
            d_str = ''
            if d > 0:
                d_str += '{} days '.format(d)
            if h > 0:
                d_str += '{} hrs '.format(h)
            if m > 0:
                d_str += '{} mins '.format(m)
            if s > 0:
                d_str += '{} secs '.format(s)
            tmp = ['\033[1;36m[{}]\033[0m \033[1;34m{}ago\033[0m'.format(one, d_str)] + item_str
            recs.setdefault(flag, []).append(tmp)
        ret_flags = sorted(list(recs.keys()))
        ret_strs = [recs[i] for i in ret_flags]
        return ret_flags, ret_strs

    def get_section_view(self, ret_str_ongoing, ret_str_ended):
        ret_str = []
        if ret_str_ongoing:
            ret_str.append('Ongoing:')
            for group_view in ret_str_ongoing:
                tag, group_view = group_view
                if group_view:
                    ret_str.append(' \033[1;35m[{}]\033[0m'.format(tag))
                    for group_line in group_view:
                        for line in group_line:
                            ret_str.append('  ' + line)
        if ret_str_ended:
            ret_str.append('Ended:')
            for group_view in ret_str_ended:
                tag, group_view = group_view
                if group_view:
                    ret_str.append(' \033[1;35m[{}]\033[0m'.format(tag))
                    for group_line in group_view:
                        for line in group_line:
                            ret_str.append('  ' + line)
        return ret_str

    def render_inspect_view(self, tag: AnyStr, name: AnyStr):
        ret_str_ongoing = []
        ret_str_ended = []
        end_flags, view_strs = self.group_panel_view(tag, inspect=name)
        for end_flag, view_str in zip(end_flags, view_strs):
            if end_flag:
                ret_str_ended.append((tag, view_str))
            else:
                ret_str_ongoing.append((tag, view_str))
        ret_str = self.get_section_view(ret_str_ongoing, ret_str_ended)
        return '\n'.join(ret_str)

    def render_panel_view(self, tags: List[AnyStr], duration=86400):
        ret_str_ongoing = []
        ret_str_ended = []
        t_set = set(tags)
        for tag in sorted(self.cfg['activated_groups'].keys()):
            if not t_set or tag in t_set:
                end_flags, view_strs = self.group_panel_view(
                    tag, duration=duration, width=self.width)
                for end_flag, view_str in zip(end_flags, view_strs):
                    if end_flag:
                        ret_str_ended.append((tag, view_str))
                    else:
                        ret_str_ongoing.append((tag, view_str))
        ret_str = self.get_section_view(ret_str_ongoing, ret_str_ended)
        return '\n'.join(ret_str)

    def add_group_by_args(self, args: Any):
        self.activate(args.tag)
        self.add_paths(args.tag, args.path)
        self.add_patterns(args.tag, args.pattern)

    def list_group_by_args(self, args: Any):
        return self.render_config_view(tags=args.tag, showPath=args.path, showPattern=args.pattern)

    def rm_group_by_args(self, args: Any):
        self.deactivate(args.tag)

    def del_group_by_args(self, args: Any):
        self.del_paths(args.tag, args.path)
        self.del_patterns(args.tag, args.pattern)

    def mv_group_by_args(self, args: Any):
        self.mv(args.oldtag, args.newtag)

    def arrange_group_by_args(self, args: Any):
        self.arrange(args.tag, args.indices)

    def config_group_by_args(self, args: Any):
        self.config(args.tag, fnp=args.file, ending=args.end, builtin_func=args.default,
                    remove_all_path=args.allpath, remove_all_patterns=args.allcfg, exclude=args.exclude, include=args.include)
        if args.pattern is not None:
            for p in args.pattern:
                self.edit_patterns(args.tag, p[0], p[1])

    def inspect_group_by_args(self, args: Any):
        return self.render_inspect_view(args.tag, args.name)

    def render_by_args(self, args: Any):
        return self.render_panel_view(args.tag if hasattr(args, 'tag') else [], duration=args.duration * 86400)

    def process(self, args: Any):
        if args.command == 'add':
            self.add_group_by_args(args)
        if args.command == 'list':
            view_str = self.list_group_by_args(args)
            print(view_str)
        if args.command == 'rm':
            self.rm_group_by_args(args)
        if args.command == 'delete':
            self.del_group_by_args(args)
        if args.command == 'rename':
            self.mv_group_by_args(args)
        if args.command == 'arrange':
            self.arrange_group_by_args(args)
        if args.command == 'config':
            self.config_group_by_args(args)
        if args.command == 'inspect':
            view_str = self.inspect_group_by_args(args)
            print(view_str)
        if args.command == 'status' or args.command is None:
            view_str = self.render_by_args(args)
            print(view_str)


def main(args):
    mgr = TaskManager()
    # mgr.process(args)
    try:
        mgr.process(args)
    except KeyError as e:
        print('Group name {} not found.'.format(e.args[0]))
    except ValueError as e:
        print('Target group name {} exists.'.format(e.args[0]))
    except IndexError as e:
        print('Invalid index {}.'.format(e.args[0]))
    except Exception as e:
        print('Unknown error.')


if __name__ == '__main__':
    parser = get_argparser()
    args = parser.parse_args()
    # print(args)
    main(args)
