#!/usr/bin/env python3
"""SQL semantics audit, independent Engine/tempfile process per case.
Usage: python3 run_probes.py /path/to/repo [output-dir]
Only builds a standalone audit runner; never modifies src. Findings are data.
"""
import json, pathlib, re, subprocess, sys, collections, shutil, tempfile, os
ROOT=pathlib.Path(sys.argv[1]).resolve()
OUT=pathlib.Path(sys.argv[2] if len(sys.argv)>2 else pathlib.Path(__file__).parent/'results').resolve()
OUT.mkdir(parents=True,exist_ok=True)
CASES=[]
base=["CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER, s TEXT)", "INSERT INTO t VALUES (1, 20, 'b'), (2, 10, 'a'), (3, NULL, 'c'), (4, 20, 'b')"]
pair=base+["CREATE TABLE u (id INTEGER PRIMARY KEY, n TEXT)","INSERT INTO u VALUES (2, 'two'), (5, 'five')"]
def c(id,code,sql,rows=None,setup=None,error=None,ordered=False,note=''):
    CASES.append(dict(id=id,feature=code,setup=base if setup is None else setup,sql=sql,expected_rows=rows,expected_error=error,ordered=ordered,note=note))
c('select_filter','E061-01','SELECT id FROM t WHERE v = 20',[['1'],['4']])
c('arithmetic','E011-04','SELECT 2 + 3',[['5']],setup=[])
c('divide_zero','E011-04','SELECT 1 / 0',error='runtime',setup=[])
c('null_eq','E131','SELECT id FROM t WHERE v = NULL',[])
c('null_neq','E131','SELECT id FROM t WHERE v <> NULL',[])
c('null_arithmetic','E131','SELECT v + 1 FROM t WHERE id = 3',[['NULL']])
c('null_predicate','E061-06','SELECT id FROM t WHERE v IS NULL',[['3']])
c('three_value_or','E061-14','SELECT id FROM t WHERE v = NULL OR id = 2',[['2']])
c('three_value_and','E061-14','SELECT id FROM t WHERE v = NULL AND id = 3',[])
c('boolean_type','T031','SELECT b FROM b',[['true'],['false'],['NULL']],setup=['CREATE TABLE b (id INTEGER PRIMARY KEY, b BOOLEAN)','INSERT INTO b VALUES (1, TRUE), (2, FALSE), (3, NULL)'])
c('distinct','E051-01','SELECT DISTINCT v FROM t',[['20'],['10'],['NULL']])
c('order_desc','F850','SELECT id FROM t ORDER BY id DESC',[['4'],['3'],['2'],['1']],ordered=True)
c('order_nonselected','E121-02','SELECT s FROM t ORDER BY id DESC',[['b'],['c'],['a'],['b']],ordered=True)
c('order_expr','E121-03','SELECT id FROM t ORDER BY 0 - id',[['4'],['3'],['2'],['1']],ordered=True)
c('order_nulls','F850','SELECT v FROM t ORDER BY v NULLS FIRST',[['NULL'],['10'],['20'],['20']],ordered=True)
c('in_list','E061-03','SELECT id FROM t WHERE id IN (1, 4)',[['1'],['4']])
c('like','E061-04',"SELECT id FROM t WHERE s LIKE 'a%'",[['2']])
c('between','E061-02','SELECT id FROM t WHERE v BETWEEN 10 AND 20',[['1'],['2'],['4']])
c('inner_join','F041-01','SELECT t.id, u.n FROM t JOIN u ON t.id = u.id',[['2','two']],setup=pair)
c('left_join','F041-03','SELECT t.id, u.n FROM t LEFT JOIN u ON t.id = u.id',[['1','NULL'],['2','two'],['3','NULL'],['4','NULL']],setup=pair)
c('right_join','F041-04','SELECT t.id, u.n FROM t RIGHT JOIN u ON t.id = u.id',[['2','two'],['NULL','five']],setup=pair)
c('full_join','F406','SELECT t.id, u.n FROM t FULL JOIN u ON t.id = u.id',[['1','NULL'],['2','two'],['3','NULL'],['4','NULL'],['NULL','five']],setup=pair)
c('aggregate_count','E091-02','SELECT COUNT(*), COUNT(v) FROM t',[['4','3']])
c('aggregate_sum','E091-05','SELECT SUM(v) FROM t',[['50']])
c('aggregate_avg','E091-01','SELECT AVG(v) FROM t',[['16.666666666666668']])
c('aggregate_empty','E091','SELECT COUNT(*), SUM(v), AVG(v), MIN(v), MAX(v) FROM t WHERE id = 99',[['0','NULL','NULL','NULL','NULL']])
c('aggregate_distinct','E091-07','SELECT COUNT(DISTINCT v) FROM t',[['2']])
c('having','E051-06','SELECT s, COUNT(*) FROM t GROUP BY s HAVING COUNT(*) > 1',[['b','2']])
c('scalar_one','F471','SELECT (SELECT v FROM t WHERE id = 1)',[['20']])
c('scalar_empty','F471','SELECT (SELECT v FROM t WHERE id = 99)',[['NULL']])
c('scalar_cardinality','F471','SELECT (SELECT v FROM t)',error='runtime')
c('correlated_exists','E061-13','SELECT id FROM t WHERE EXISTS (SELECT id FROM u WHERE u.id = t.id)',[['2']],setup=pair)
c('in_subquery','E061-11','SELECT id FROM t WHERE id IN (SELECT id FROM u)',[['2']],setup=pair)
c('in_subquery_null','E061-11','SELECT id FROM t WHERE v IN (SELECT v FROM t WHERE id = 3)',[])
c('union','E071-01','SELECT id FROM t WHERE id = 1 UNION SELECT id FROM t WHERE id = 1',[['1']])
c('union_all','E071-02','SELECT id FROM t WHERE id = 1 UNION ALL SELECT id FROM t WHERE id = 1',[['1'],['1']])
c('except','E071-03','SELECT id FROM t EXCEPT SELECT id FROM u',[['1'],['3'],['4']],setup=pair)
c('intersect','F302','SELECT id FROM t INTERSECT SELECT id FROM u',[['2']],setup=pair)
c('cast','F201',"SELECT CAST('12' AS INTEGER)",[['12']],setup=[])
c('decimal','E011-03','SELECT n FROM d',[['0.30']],setup=['CREATE TABLE d (id INTEGER PRIMARY KEY, n DECIMAL(10,2))','INSERT INTO d VALUES (1, 0.30)'])
c('varchar','E021-02','SELECT n FROM d', [['ok']],setup=['CREATE TABLE d (id INTEGER PRIMARY KEY, n VARCHAR(10))',"INSERT INTO d VALUES (1, 'ok')"])
c('char_padding','E021-01','SELECT n FROM d',[['a  ']],setup=['CREATE TABLE d (id INTEGER PRIMARY KEY, n CHAR(3))',"INSERT INTO d VALUES (1, 'a')"])
c('date_storage','F051-01','SELECT n FROM d',[['2024-02-29']],setup=['CREATE TABLE d (id INTEGER PRIMARY KEY, n DATE)',"INSERT INTO d VALUES (1, '2024-02-29')"],note='Storage subset only: typed literal tested separately')
c('date_literal','F051-01',"SELECT DATE '2024-02-29'",[['2024-02-29']],setup=[])
c('bad_date','F051-01',"INSERT INTO d VALUES (1, '2024-02-30')",error='runtime',setup=['CREATE TABLE d (id INTEGER PRIMARY KEY, n DATE)'])
c('timestamp_literal','F051-03',"SELECT TIMESTAMP '2024-02-29 12:30:00.123456'",[['2024-02-29 12:30:00.123456']],setup=[])
c('not_null','E141-01','INSERT INTO d VALUES (1, NULL)',error='runtime',setup=['CREATE TABLE d (id INTEGER PRIMARY KEY, n INTEGER NOT NULL)'])
c('pk_duplicate','E141-03','INSERT INTO t VALUES (1, 99, \'x\')',error='runtime')
c('pk_null','E141-08',"INSERT INTO t VALUES (NULL, 99, 'x')",error='runtime')
c('unique','E141-02','INSERT INTO d VALUES (2, 1)',error='runtime',setup=['CREATE TABLE d (id INTEGER PRIMARY KEY, n INTEGER NOT NULL UNIQUE)','INSERT INTO d VALUES (1, 1)'])
c('check','E141-06','INSERT INTO d VALUES (1, -1)',error='runtime',setup=['CREATE TABLE d (id INTEGER PRIMARY KEY, n INTEGER CHECK (n > 0))'])
c('composite_pk','E141-03','INSERT INTO d VALUES (1, 2)',error='runtime',setup=['CREATE TABLE d (a INTEGER, b INTEGER, PRIMARY KEY (a,b))','INSERT INTO d VALUES (1, 2)'])
fk=['CREATE TABLE p (id INTEGER PRIMARY KEY)','CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES p(id))','INSERT INTO p VALUES (1)','INSERT INTO ch VALUES (1, 1)']
c('fk_insert','E141-04','INSERT INTO ch VALUES (2, 99)',error='runtime',setup=fk)
c('fk_delete_no_action','E141-04','DELETE FROM p WHERE id = 1',error='runtime',setup=fk)
c('fk_update_no_action','E141-04','UPDATE p SET id = 2 WHERE id = 1',error='runtime',setup=fk)
c('fk_delete_cascade','F191','SELECT id FROM ch',[],setup=[s.replace('REFERENCES p(id))','REFERENCES p(id) ON DELETE CASCADE)') for s in fk]+['DELETE FROM p WHERE id = 1'])
c('fk_update_cascade','F701','SELECT pid FROM ch',[['2']],setup=[s.replace('REFERENCES p(id))','REFERENCES p(id) ON UPDATE CASCADE)') for s in fk]+['UPDATE p SET id = 2 WHERE id = 1'])
c('default','E141-07','SELECT n FROM d',[['7']],setup=['CREATE TABLE d (id INTEGER PRIMARY KEY, n INTEGER DEFAULT 7)','INSERT INTO d (id) VALUES (1)'])
c('alter_add','F031-04','SELECT n FROM t',[['NULL']]*4,setup=base+['ALTER TABLE t ADD COLUMN n INTEGER'])
c('view','F031-02','SELECT id FROM vv',[['1']],setup=base+['CREATE VIEW vv AS SELECT id FROM t WHERE id = 1'])
c('drop_table','F031-13','SELECT id FROM t',error='runtime',setup=base+['DROP TABLE t RESTRICT'])
c('rollback','E151-02','SELECT id FROM t', [['1'],['2'],['3'],['4']],setup=base+['BEGIN',"INSERT INTO t VALUES (5, 30, 'x')",'ROLLBACK'])
c('commit','E151-01','SELECT id FROM t WHERE id = 5',[['5']],setup=base+['BEGIN',"INSERT INTO t VALUES (5, 30, 'x')",'COMMIT'])
c('savepoint','T271','SELECT id FROM t WHERE id = 5',[],setup=base+['BEGIN','SAVEPOINT s',"INSERT INTO t VALUES (5, 30, 'x')",'ROLLBACK TO SAVEPOINT s','COMMIT'])
c('serializable','E152-01','SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',setup=['BEGIN'])
c('readonly','E152-02','SET TRANSACTION READ ONLY',setup=['BEGIN'])
c('information_schema','F021-02','SELECT table_name FROM information_schema.tables', [['t']])
c('case','F261-02','SELECT CASE WHEN 1 = 1 THEN 7 ELSE 9 END',[['7']],setup=[])
c('coalesce','F261-04','SELECT COALESCE(NULL, 7)',[['7']],setup=[])
c('comment','E161','-- audit comment SELECT 99',setup=[],error='parse',note='Full-line comment should be ignored; runner exposes no empty-statement API')
c('inline_comment','E161','SELECT 1 -- audit comment',[['1']],setup=[])
c('cte','T121','WITH q AS (SELECT id FROM t) SELECT id FROM q',[['1'],['2'],['3'],['4']])
c('window','T611','SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM t',[['1','1'],['2','2'],['3','3'],['4','4']])
c('json_value','T821','SELECT JSON_VALUE(\'{"a":1}\', \'$.a\')',[['1']],setup=[],note='Optional JSON smoke; feature code deliberately unmapped pending source verification')
c('update_null','E131','SELECT v FROM t WHERE id = 2',[['NULL']],setup=base+['UPDATE t SET v = NULL WHERE id = 2'])
c('update_expression','E101-03','SELECT v FROM t WHERE id = 2',[['11']],setup=base+['UPDATE t SET v = v + 1 WHERE id = 2'])
c('pk_text','E141-03','SELECT id FROM d',[['a  ']],setup=['CREATE TABLE d (id CHAR(3) PRIMARY KEY)',"INSERT INTO d VALUES ('a')"])
c('multi_insert_atomic','E101-01','SELECT id FROM t', [['1'],['2'],['3'],['4']],setup=base+['!INSERT INTO t VALUES (5, 30, \'x\'), (1, 99, \'dup\')'])
c('update','E101-03','SELECT v FROM t WHERE id = 2',[['15']],setup=base+['UPDATE t SET v = 15 WHERE id = 2'])
c('delete','E101-04','SELECT id FROM t',[['1'],['3'],['4']],setup=base+['DELETE FROM t WHERE id = 2'])

# Additional adversarial cases prevent accidental passes from ignored suffixes.
c('union_second_branch','E071-01','SELECT id FROM t WHERE id = 1 UNION SELECT id FROM t WHERE id = 2',[['1'],['2']])
c('union_duplicate_elimination','E071-01','SELECT v FROM t WHERE id = 1 UNION SELECT v FROM t WHERE id = 4',[['20']])
c('composite_pk_distinct','E141-03','SELECT a, b FROM d',[['1','2'],['1','3']],setup=['CREATE TABLE d (a INTEGER, b INTEGER, PRIMARY KEY (a,b))','INSERT INTO d VALUES (1, 2), (1, 3)'])
c('fk_nullable','E141-04','SELECT pid FROM ch',[['1'],['NULL']],setup=fk+['INSERT INTO ch VALUES (2, NULL)'])
c('fk_composite_invalid','E141-04','INSERT INTO ch VALUES (1, 1, 999)',error='runtime',setup=['CREATE TABLE p (a INTEGER, b INTEGER, PRIMARY KEY (a,b))','INSERT INTO p VALUES (1, 2)','CREATE TABLE ch (id INTEGER PRIMARY KEY, a INTEGER, b INTEGER, FOREIGN KEY (a,b) REFERENCES p(a,b))'])
c('fk_nonphysical_parent_key','E141-04','SELECT pid FROM ch',[['100']],setup=['CREATE TABLE p (id INTEGER, n INTEGER PRIMARY KEY)','INSERT INTO p VALUES (1, 100)','CREATE TABLE ch (id INTEGER PRIMARY KEY, pid INTEGER, FOREIGN KEY (pid) REFERENCES p(n))','INSERT INTO ch VALUES (1, 100)'])
c('drop_restrict_dependency','F031-13','DROP TABLE p RESTRICT',error='runtime',setup=fk)
c('cursor','E121-01','DECLARE cur CURSOR FOR SELECT id FROM t')
c('exists_uncorrelated','E061-08','SELECT id FROM t WHERE EXISTS (SELECT id FROM u)',[['1'],['2'],['3'],['4']],setup=pair)
c('all_null_aggregate','E091','SELECT COUNT(v), SUM(v), AVG(v), MIN(v), MAX(v) FROM t WHERE id = 3',[['0','NULL','NULL','NULL','NULL']])
c('delete_all','E101-04','SELECT id FROM t',[],setup=base+['DELETE FROM t'])
c('bad_sql_tail','E051','SELECT id FROM t WHERE id = 1 NONSENSE',error='runtime',note='Invalid syntax must be rejected; special parse oracle below')
error_matches={'pk_duplicate':'DuplicateKey','composite_pk':'DuplicateKey','not_null':'NullViolation','pk_null':'NullViolation','bad_date':'not a valid DATE|Invalid DATE','fk_insert':'ForeignKeyViolation','fk_delete_no_action':'ForeignKeyViolation','fk_update_no_action':'ForeignKeyViolation','fk_composite_invalid':'ForeignKeyViolation','drop_table':'No such table','drop_restrict_dependency':'referenc|depend|ForeignKey','divide_zero':'zero|division','scalar_cardinality':'cardinal|more than one|multiple'}
for case in CASES:
    case['expected_error_pattern']=error_matches.get(case.get('id'))
    if case.get('id')=='json_value': case['note']='Optional JSON_VALUE smoke, T821 subset only'
    if case.get('id')=='comment':
        case['expected_error']='comment_api'
    if case.get('id')=='bad_sql_tail': case['expected_error']='syntax'

def assess(case,steps):
    # Valid declarations with broken execution are bugs, not syntax gaps.
    if case.get('id') in ('fk_nullable','fk_nonphysical_parent_key','fk_update_cascade','composite_pk_distinct','update_null'):
        if any(s['status']=='EXEC_ERROR' for s in steps): return ('bug','valid_statement_rejected')
    if case.get('id')=='comment': return ('unverified','empty_statement_api')
    for i, s in enumerate(steps[:-1]):
        allowed=case['setup'][i].startswith('!')
        if s['status']!='OK' and not (allowed and s['status']=='EXEC_ERROR'):
            return ('unsupported' if s['status']=='PARSE_ERROR' else 'partial','setup_'+s['status'].lower())
        if allowed and s['status']=='OK': return ('bug','expected_setup_error_missing')
    s=steps[-1]
    if s['status']=='PARSE_ERROR':
        if case['expected_error']=='syntax': return ('implemented','invalid_syntax_rejected')
        return ('unsupported','syntax_rejected')
    if case['expected_error']:
        if case['expected_error']=='runtime' and s['status']=='EXEC_ERROR':
            pat=case.get('expected_error_pattern')
            if pat and not re.search(pat,s.get('error') or '',re.I): return ('bug','wrong_error')
            return ('implemented','expected_error_enforced')
        if case['expected_error']=='parse' and s['status']=='PARSE_ERROR': return ('unverified','comment_api')
        return ('bug','expected_error_missing')
    if s['status']=='EXEC_ERROR':
        if case.get('id') in ('distinct','window','information_schema'): return ('unsupported','runtime_feature_absent')
        return ('partial','runtime_rejected')
    if case['expected_rows'] is None: return ('unverified','no_result_oracle')
    a=s['rows']; e=case['expected_rows']
    if case['ordered']: same=a==e
    else: same=collections.Counter(map(tuple,a))==collections.Counter(map(tuple,e))
    return ('implemented','result_matches') if same else ('bug','wrong_result')

# Test classifier before launching probes: SQL errors must not become passes.
assert assess(dict(setup=[],expected_error=None,expected_rows=[['1']],ordered=False),[dict(status='EXEC_ERROR')])==('partial','runtime_rejected')
assert assess(dict(setup=[],expected_error=None,expected_rows=[['1']],ordered=False),[dict(status='OK',rows=[['2']])])==('bug','wrong_result')
assert assess(dict(setup=[],expected_error='runtime',expected_rows=None,ordered=False),[dict(status='OK',rows=[])])==('bug','expected_error_missing')
subprocess.run(['cargo','--version'],check=True,capture_output=True)
runner_dir=tempfile.TemporaryDirectory(prefix='aerodb-audit-runner-')
runner=pathlib.Path(runner_dir.name)
(runner/'src').mkdir()
(runner/'Cargo.toml').write_text('[package]\nname="aerodb-sql-audit-runner"\nversion="0.0.0"\nedition="2024"\n[dependencies]\naerodb={path='+json.dumps(str(ROOT))+'}\ntempfile="3"\n')
shutil.copyfile(pathlib.Path(__file__).with_name('sql_audit.rs'),runner/'src/main.rs')
env=dict(os.environ,CARGO_TARGET_DIR=str(OUT/'.runner-target'))
subprocess.run(['cargo','build','--manifest-path',str(runner/'Cargo.toml'),'--offline'],cwd=ROOT,env=env,check=True)
exe=OUT/'.runner-target/debug/aerodb-sql-audit-runner'
results=[]
for case in CASES:
    sqls=[x.removeprefix('!') for x in case['setup']]+[case['sql']]
    p=subprocess.run([str(exe)],input='\n'.join(sqls)+'\n',text=True,capture_output=True,timeout=30)
    (OUT/(case.get('id')+'.log')).write_text(p.stdout+'\nSTDERR:\n'+p.stderr)
    steps=[]
    for i,body in re.findall(r'@@BEGIN (\d+)\n(.*?)@@END \1\n',p.stdout,re.S):
        output,status=body.rsplit('@@STATUS ',1)
        kind,*error=status.strip().split(' ',1)
        lines=output.splitlines()
        query=sqls[int(i)].lstrip().upper().startswith(('SELECT','WITH'))
        steps.append(dict(sql=sqls[int(i)],status=kind,error=error[0] if error else None,output=output,rows=[x.split(' | ') for x in lines[1:]] if query else None))
    if p.returncode or len(steps)!=len(sqls): status,reason='unverified','harness_process_failure'
    else: status,reason=assess(case,steps)
    results.append(dict(**case,status=status,reason=reason,steps=steps,returncode=p.returncode))
    print(case.get('id'),status,reason,steps[-1].get('rows') if steps else p.stderr)
head=subprocess.check_output(['git','rev-parse','HEAD'],cwd=ROOT,text=True).strip()
summary=dict(head=head,version='0.5.1',case_count=len(results),sql_statement_count=sum(len(x['steps']) for x in results),statuses=dict(collections.Counter(x['status'] for x in results)),reasons=dict(collections.Counter(x['reason'] for x in results)))
(OUT/'cases.json').write_text(json.dumps(CASES,ensure_ascii=False,indent=2)+'\n')
(OUT/'results.json').write_text(json.dumps(dict(summary=summary,cases=results),ensure_ascii=False,indent=2)+'\n')
print(json.dumps(summary,indent=2))
