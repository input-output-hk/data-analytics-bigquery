import split_long_lists as s
from io import StringIO

def test_split_list_in_file():
    infile = StringIO()
    outfile = StringIO()
    csvTxt = """304,addr1q8002va9kyu2pxskjgcy28pjddgv5pzw0aardg8mr7fcg85esc3dq7jrquccaf9nkjshwj6x39jr0z42jkvkhl5c3gxsvnjr97,\"[\"\"(27562275,14,0)\"\", \"\"(28057950,11,2)\"\", \"\"(28071774,6,0)\"\", \"\"(28576019,1,4)\"\", \"\"(30994309,7,0)\"\", \"\"(38506769,26,0)\"\", \"\"(39891958,33,0)\"\", \"\"(40367394,13,1)\"\", \"\"(40704258,20,1)\"\", \"\"(43014254,39,0)\"\"]\""""
    infile.write(csvTxt)
    infile.seek(0)
    list_index = 2
    max_list_length = 3
    s.split_list_in_file(list_index, max_list_length, infile, outfile)
    outfile.seek(0)
    actual = outfile.read()
    print(f"RES:\n{actual}")
    expected = """304,addr1q8002va9kyu2pxskjgcy28pjddgv5pzw0aardg8mr7fcg85esc3dq7jrquccaf9nkjshwj6x39jr0z42jkvkhl5c3gxsvnjr97,\"[\"\"(27562275,14,0)\"\",\"\"(28057950,11,2)\"\",\"\"(28071774,6,0)\"\"]\"
304,addr1q8002va9kyu2pxskjgcy28pjddgv5pzw0aardg8mr7fcg85esc3dq7jrquccaf9nkjshwj6x39jr0z42jkvkhl5c3gxsvnjr97,\"[\"\"(28576019,1,4)\"\",\"\"(30994309,7,0)\"\",\"\"(38506769,26,0)\"\"]\"
304,addr1q8002va9kyu2pxskjgcy28pjddgv5pzw0aardg8mr7fcg85esc3dq7jrquccaf9nkjshwj6x39jr0z42jkvkhl5c3gxsvnjr97,\"[\"\"(39891958,33,0)\"\",\"\"(40367394,13,1)\"\",\"\"(40704258,20,1)\"\"]\"
304,addr1q8002va9kyu2pxskjgcy28pjddgv5pzw0aardg8mr7fcg85esc3dq7jrquccaf9nkjshwj6x39jr0z42jkvkhl5c3gxsvnjr97,\"[\"\"(43014254,39,0)\"\"]\"
"""
    assert actual == expected
